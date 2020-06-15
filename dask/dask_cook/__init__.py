import asyncio
import json
import logging
import time

from datetime import timedelta
from threading import Thread
from typing import Union

from cookclient import JobClient
from cookclient.jobs import (
    Application,
    Job,
    State as JobState,
    Status as JobStatus,
)

from distributed.deploy import ProcessInterface

VERSION = '0.1.0'

_LOG = logging.getLogger(__name__)


class CookJobException(Exception):
    """An exception associated with a job on Cook."""
    def __init__(self, message: str, job: Job):
        super().__init__(message)
        self.job = job

    def __str__(self):
        return f"CookJobException: {self.message}\nJob: {str(self.job)}"

    def __repr__(self):
        return f"CookJobException(message={self.message}, job={repr(self.job)})"  # noqa: E501


class CookJob(ProcessInterface):
    """An instance of a job running on Cook.

    :param client: Client instance through which requests should be made.
    :type client: JobClient
    :param scheduler: Address of the Dask scheduler.
    :type scheduler: str
    :param name: Name of this process.
    :type name: str
    :param jobspec: Job specification to use when submitting to Cook. For
        further details, see ``JobClient.submit``.
    :type jobspec: dict
    :param init_kill_poll_frequency: Time to wait in between job status poll
        invocations during job init and kill. If a scalar is provided, then
        the value is interpreted as a number of seconds. Defaults to 5s.
    :type init_kill_poll_frequency: float or timedelta
    :param monitor_poll_frequency: Time to wait in between job status poll
        invocations inside the monitor thread. If a scalar is provided, then
        the value is interpreted as a number of seconds. Defaults to 30s.
    """

    def __init__(self, *,
                 client: JobClient,
                 scheduler: str,
                 name: str,
                 jobspec: dict,
                 init_kill_poll_frequency: Union[float, timedelta] = 5,
                 monitor_poll_frequency: Union[float, timedelta] = 30):
        # Convert timedelta objects to seconds
        if isinstance(init_kill_poll_frequency, timedelta):
            init_kill_poll_frequency = init_kill_poll_frequency.total_seconds()
        if isinstance(monitor_poll_frequency, timedelta):
            monitor_poll_frequency = monitor_poll_frequency.total_seconds()

        self.client = client
        self.jobspec = jobspec
        self.uuid = None
        self.init_kill_poll_frequency = init_kill_poll_frequency
        self.monitor_poll_frequency = monitor_poll_frequency
        self.address = None
        super().__init__(scheduler, name)

    async def start(self):
        """Submit the job and wait for it to start.

        This function will submit the jobspec provided on construction to the
        remote Cook instance using the provided client, and then periodically
        poll the job until it is running. Once running, this function will also
        kick off the job monitor thread.

        If the job exits for any reason during the polling period, then this
        function will raise an exception, as the job is expected to be a
        long-running process on Cook.
        """
        await self._start_helper()
        Thread(self._monitor_thread_main).start()
        await super().start()

    async def _start_helper(self):
        """Submit the job and wait for it to start.

        This function implements the core logic of ``start()``, but does not
        actually start the monitoring thread.
        """
        _LOG.debug(f"Submitting jobspec: {json.dumps(self.jobspec, indent=4)}")
        self.uuid = self.client.submit(**self.jobspec)
        _LOG.debug(f"Jobspec submitted, got UUID {self.uuid}")

        started = False
        while not started:
            job = self.client.query(self.uuid)
            if job.status == JobStatus.WAITING:
                _LOG.debug(f"Job {self.uuid} hasn't started yet, polling again in {self.init_kill_poll_frequency}s...")  # noqa: E501
                await asyncio.sleep(self.init_kill_poll_frequency)
            else:
                started = True

        if job.status == JobStatus.COMPLETED:
            if job.state == JobState.PASSED:
                raise CookJobException("Job succeeded unexpectedly", job)
            elif job.state == JobState.FAILED:
                raise CookJobException("Job failed", job)

        self.status = 'running'
        self.address = f'https://{job.instances[0].hostname}'
        _LOG.info(f"Job {self.uuid} is now running and listening on {self.address}")  # noqa: E501

    async def close(self):
        """Kill the remote job instance and wait for it to finish.

        This function will also set this ``CookJob`` instance's status
        appropriately so as to signal the monitoring thread to shutdown.
        """
        _LOG.info(f"Killing job {self.uuid}")
        self.status = 'closing'
        self.client.kill(self.uuid)
        killed = False
        while not killed:
            job = self.client.query(self.uuid)
            if job.status != JobStatus.COMPLETED:
                _LOG.debug(f"Job {self.uuid} hasn't died yet, polling again in {self.init_kill_poll_frequency}s...")  # noqa: E501
            else:
                killed = True

        _LOG.info(f"Job {self.uuid} killed")
        await super().close()

    def _monitor_thread_main(self):
        """Monitor the job running on Cook.

        This will query the job status from Cook and restart the job if the
        job dies. Will exit once the job's status is set to ``closing`` or
        ``closed``.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        while True:
            if self.status in ('closing', 'closed'):
                _LOG.debug(f"Job {self.uuid} is ${self.status}; exiting monitoring thread.")  # noqa: E501
                break

            job = self.client.query(self.uuid)
            if job.status != JobStatus.RUNNING:
                _LOG.warn(f"Job {self.uuid} died, restarting...")
                loop.run_until_complete(self._locked_start())

            time.sleep(self.monitor_poll_frequency)

        loop.close()

    async def _locked_restart(self):
        """Lock this instance and restart the job."""
        async with self.lock.acquire():
            self.status = 'restarting'
            await self.start()
            self.status = 'running'


class Worker(CookJob):
    _DEFAULT_JOBSPEC = {
        'application': Application('dask_cook', VERSION)
    }

    def __init__(self, *,
                 client: JobClient,
                 scheduler: str,
                 name: str,
                 init_kill_poll_frequency: Union[float, timedelta] = 5,
                 monitor_poll_frequency: Union[float, timedelta] = 30,
                 jobspec: dict = {}):
        super().__init__(client=client,
                         scheduler=scheduler,
                         name=name,
                         init_kill_poll_frequency=init_kill_poll_frequency,
                         monitor_poll_frequency=monitor_poll_frequency,
                         jobspec=jobspec)
        pass
