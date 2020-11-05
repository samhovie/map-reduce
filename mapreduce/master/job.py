import logging
import mapreduce.utils
from pathlib import Path


class Job:
    _next_id = 0
    _current = None

    def current():
        return Job._current


    def __init__(
            self,
            input_dir,
            output_dir,
            mapper_exec,
            reducer_exec,
            num_mappers,
            num_reducers):
        self._status = "waiting"
        self._input_dir = input_dir
        self._output_dir = output_dir
        self._mapper_exec = mapper_exec
        self._reducer_exec = reducer_exec
        self._num_mappers = num_mappers
        self._num_reducers = num_reducers

        self._id = Job._next_id
        Job._next_id += 1

        logging.info(f"Master: Received job {self._id}: {input_dir} "
                     f"{output_dir} {mapper_exec} {reducer_exec} "
                     f"{num_mappers} {num_reducers}"
                    )

        print("Creating dirs")
        self._folder = Path("tmp")/f"job-{self._id}"

        self._folder.mkdir()
        (self._folder/"mapper-output").mkdir()
        (self._folder/"grouper-output").mkdir()
        (self._folder/"reducer-output").mkdir()

    def start(self):
        logging.info(f"Master: Starting job {self._id}")

        assert(Job._current is None)
        Job._current = self

        self.status = "started"
        self.mapping()

    def mapping(self):
        # TODO: Partition input files for this stage
        logging.info(f"Master: Starting mapping stage for job {self._id}")

    def grouping(self):
        # TODO
        logging.info(f"Master: Starting grouping stage for job {self._id}")

    def reducing(self):
        # TODO
        logging.info(f"Master: Starting reducing stage for job {self._id}")
    
    def cleanup(self):
        # TODO
        logging.info(f"Master: Finishing job {self._id}")
        self.status = "finished"
        Job._current = None

    def status_update(self, worker_status):
        # TODO
        pass

