import logging
import os

import trio
import trio.abc


logger = logging.getLogger(__name__)


def configure_logging(verbosity):
    instruments = []
    logging_config = {
        'format': '%(levelname)-8s [%(asctime)s] %(name)-30s - %(message)s',
        'datefmt': '%Y-%m-%d %H:%M:%S'
        }
    if verbosity == 1:
        logging_config['level'] = logging.INFO
        os.environ['HTTPX_LOG_LEVEL'] = 'DEBUG'
    if verbosity == 2:
        logging_config['level'] = logging.DEBUG
        instruments.append(Tracer())
        os.environ['HTTPX_LOG_LEVEL'] = 'TRACE'
    logging.basicConfig(**logging_config)
    return instruments


class Tracer(trio.abc.Instrument):
    """A trio instrument to trace async activity

    Shamelessly ripped off from the trio tutorial
    https://trio.readthedocs.io/en/stable/tutorial.html#task-switching-illustrated
    """
    def before_run(self):
        logger.debug('!!! run started')

    def _print_with_task(self, msg, task):
        # repr(task) is perhaps more useful than task.name in general,
        # but in context of a tutorial the extra noise is unhelpful.
        logger.debug(f'{repr(task)}: {msg}')

    def task_spawned(self, task):
        self._print_with_task('new task spawned', task)

    def task_scheduled(self, task):
        self._print_with_task('task scheduled', task)

    def before_task_step(self, task):
        self._print_with_task('about to run one step of task', task)

    def after_task_step(self, task):
        self._print_with_task('task step finished', task)

    def task_exited(self, task):
        self._print_with_task('task exited', task)

    def before_io_wait(self, timeout):
        if timeout:
            logger.debug(f'waiting for I/O for up to {timeout} seconds')
        else:
            logger.debug('doing a quick check for I/O')
        self._sleep_time = trio.current_time()

    def after_io_wait(self, timeout):
        duration = trio.current_time() - self._sleep_time
        logger.debug(f'finished I/O check (took {duration} seconds)')

    def after_run(self):
        logger.debug('run finished')
