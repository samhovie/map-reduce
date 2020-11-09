import logging
import mapreduce.utils
from pathlib import Path


class Job:
    _next_id = 0

    def __init__(
            self,
            input_dir,
            output_dir,
            mapper_exec,
            reducer_exec,
            num_mappers,
            num_reducers,
            workers):
        self._status = "waiting"
        self._input_dir = Path(input_dir)
        self._output_dir = Path(output_dir)
        self._mapper_exec = mapper_exec
        self._reducer_exec = reducer_exec
        self._num_mappers = num_mappers
        self._num_reducers = num_reducers
        self._workers = workers

        self._id = Job._next_id
        Job._next_id += 1

        logging.info(f"Master: Received job {self._id}: {input_dir} "
                     f"{output_dir} {mapper_exec} {reducer_exec} "
                     f"{num_mappers} {num_reducers}"
                    )

        self._folder = Path("tmp")/f"job-{self._id}"

        self._folder.mkdir()
        (self._folder/"mapper-output").mkdir()
        (self._folder/"grouper-output").mkdir()
        (self._folder/"reducer-output").mkdir()


    def start(self):
        logging.info(f"Master: Starting job {self._id}")

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

    def status_update(self, worker_status):
        # TODO
        pass

    def partition_input(self):
        assert(self._num_mappers != 0)

        files = self._input_dir.glob("*")
        partition = [[]*1 for i in range(self._num_mappers)]

        i = 0
        for file in files:
            partition[i].append(file)
            i = (i + 1) % len(files)
        
        return partition

