import logging
import mapreduce.utils
from pathlib import Path
import time


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
            workers,
            signals):
        self._status = "waiting"
        self._input_dir = Path(input_dir)
        self._output_dir = Path(output_dir)
        self._mapper_exec = mapper_exec
        self._reducer_exec = reducer_exec
        self._num_mappers = num_mappers
        self._num_reducers = num_reducers
        self._workers = workers
        self._signals = signals

        self._id = Job._next_id
        Job._next_id += 1

        logging.info(f"Master: Received job {self._id}: {input_dir} "
                     f"{output_dir} {mapper_exec} {reducer_exec} "
                     f"{num_mappers} {num_reducers}"
                    )

        self._folder = Path("tmp")/f"job-{self._id}"
        self._folder.mkdir()

        self._mapper_output_dir = self._folder/"mapper-output"
        self._mapper_output_dir.mkdir()
        self._grouper_output_dir = self._folder/"grouper-output"
        self._grouper_output_dir.mkdir()
        self._reducer_output_dir = self._folder/"reducer-output"
        self._reducer_output_dir.mkdir()


    def start(self):
        logging.info(f"Master: Starting job {self._id}")

        self._status = "started"
        self.mapping()
        # TODO: Maybe more stuff here

    def mapping(self):
        logging.info(f"Master: Starting mapping stage for job {self._id}")
        partition = self.partition_input()

        """
        TODO:
         - Assign as many tasks as we can to available workers
         - If not enough workers are available, "wait" (how?)
         - If there are too many workers, don't assign all of them (we don't need them all)
        """

        # Each job is a tuple containing the list of input files, the PID of the worker
        # assigned to the job, and whether the job is finished.
        job_list = [(job, None, False) for job in partition]
        job_outputs = []

        logging.info("Assigning workers for mapping")
        while len([job for job, pid, completed in job_list if not completed]) != 0:
            if self._signals["shutdown"]:
                logging.info("Shutting down in mapping stage.")
                return False
            for i, (job, worker_pid, completed) in enumerate(job_list):
                if completed:
                    continue
                elif worker_pid is not None and self._workers[worker_pid]["status"] == "ready":
                    # The worker has completed this job.
                    job_list[i] = (job, worker_pid, True)
                    assert(self._workers[worker_pid]["job_output"] is not None)
                    job_outputs.append(self._workers[worker_pid]["job_output"])
                    logging.info(f"Mapping job {i} complete")
                elif worker_pid is None:
                    for worker in self._workers.values():
                        if worker["status"] == "ready":
                            worker["status"] = "busy"
                            job_list[i] = (job, worker["pid"], False)
                            mapreduce.utils.send_message({
                                "message_type": "new_worker_job",
                                "input_files": [str(file) for file in job],
                                "executable": self._mapper_exec,
                                "output_directory": str(self._mapper_output_dir),
                                "worker_pid": worker_pid,
                            }, worker["host"], worker["port"])
                            break
                elif self._workers[worker_pid]["status"] == "dead":
                    job_list[i] = (job, None, False)

            time.sleep(0.1)
        
        logging.info("Mapping stage complete.")


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

    def partition_input(self):
        assert(self._num_mappers != 0)

        files = self._input_dir.glob("*")
        partition = [[]*1 for i in range(self._num_mappers)]

        i = 0
        for file in files:
            partition[i].append(file)
            i = (i + 1) % self._num_mappers
        
        return partition

