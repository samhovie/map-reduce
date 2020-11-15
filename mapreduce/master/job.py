import heapq
import itertools
import logging
import mapreduce.utils
from pathlib import Path
import shutil
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

        succeeded, output_files = self.mapping()
        if not succeeded:
            logging.debug("Exiting early after mapping")
            return

        if not self.grouping(output_files):
            logging.debug("Exiting early after grouping")
            return

        succeeded, reducing_output_files = self.reducing()
        if not succeeded:
            logging.debug("Exiting early after reducing")
            return

        self.cleanup(reducing_output_files)

    def mapping(self):
        logging.info(f"Master: Starting mapping stage for job {self._id}")

        get_file_name = lambda path: path.name
        files = list(self._input_dir.glob("*"))
        files.sort(key=get_file_name)
        partition = self.partition_input(files, self._num_mappers)


        # Each job is a tuple containing the list of input files, the PID of the worker
        # assigned to the job, and whether the job is finished.
        job_list = [(job, None, False) for job in partition if len(job) != 0]
        job_outputs = []

        logging.info("Assigning workers for mapping")
        while len([job for job, pid, completed in job_list if not completed]) != 0:
            if self._signals["shutdown"]:
                logging.info("Shutting down in mapping stage.")
                return False, job_outputs
            for i, (job, worker_pid, completed) in enumerate(job_list):
                if completed:
                    continue
                elif worker_pid is not None and self._workers[worker_pid]["status"] == "ready":
                    # The worker has completed this job.
                    job_list[i] = (job, worker_pid, True)
                    assert(self._workers[worker_pid]["job_output"] is not None)
                    job_outputs += self._workers[worker_pid]["job_output"]
                    logging.info(f"Mapping job {i} complete")
                    logging.info("%s outputs from job", len(self._workers[worker_pid]["job_output"]))
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
                                "worker_pid": worker["pid"],
                            }, worker["host"], worker["port"])
                            break
                elif self._workers[worker_pid]["status"] == "dead":
                    job_list[i] = (job, None, False)

            time.sleep(0.1)
        
        logging.info("Mapping stage complete.")
        logging.info("%s outputs from mapping", len(job_outputs))
        return True, job_outputs


    def grouping(self, output_files):
        logging.info(f"Master: Starting grouping stage for job {self._id}")

        output_files.sort()
        ready_workers = [worker for worker in self._workers.values() if worker["status"] == "ready"]
        partition = self.partition_input(output_files, len(ready_workers))

        # Each job is a tuple containing the list of input files, the PID of the worker
        # assigned to the job, and whether the job is finished.
        job_list = [(job, None, False) for job in partition if len(job) != 0]
        logging.info(f"Grouping jobs: {job_list}")
        job_outputs = []

        logging.info("Assigning workers for grouping")
        while len([job for job, pid, completed in job_list if not completed]) != 0:
            if self._signals["shutdown"]:
                logging.info("Shutting down in grouping stage.")
                return False
            for i, (job, worker_pid, completed) in enumerate(job_list):
                if completed:
                    continue
                elif worker_pid is not None and self._workers[worker_pid]["status"] == "ready":
                    # The worker has completed this job.
                    job_list[i] = (job, worker_pid, True)
                    assert(self._workers[worker_pid]["job_output"] is not None)
                    job_outputs += self._workers[worker_pid]["job_output"]
                    logging.info(f"grouping job {i} complete")
                elif worker_pid is None:
                    for worker in self._workers.values():
                        if worker["status"] == "ready":
                            worker["status"] = "busy"
                            job_list[i] = (job, worker["pid"], False)
                            mapreduce.utils.send_message({
                                "message_type": "new_sort_job",
                                "input_files": [str(file) for file in job],
                                "output_file": str(self._grouper_output_dir/f"sorted{(i + 1):02}"),
                                "worker_pid": worker["pid"],
                            }, worker["host"], worker["port"])
                            break
                elif self._workers[worker_pid]["status"] == "dead":
                    job_list[i] = (job, None, False)

            time.sleep(0.1)
        
        files = [Path(file).open("r") for file in job_outputs]
        sorted_lines = heapq.merge(*files)
        
        # Open reducer files
        reducer_files = []
        for i in range(self._num_reducers):
            reducer_files.append(Path(self._grouper_output_dir/f"reduce{(i + 1):02}").open("w"))
        

        get_key = lambda line: line.split("\t")[0]
        i = 0
        grouped_lines = itertools.groupby(sorted_lines, key=get_key)
        for key, group in grouped_lines:
            for line in group:
                reducer_files[i].writelines([line])
            i = (i + 1) % (self._num_reducers)

        for file in reducer_files:
            file.close()

        for file in files:
            file.close()

        logging.info("Grouping stage complete.")
        return True

    def reducing(self):
        logging.info(f"Master: Starting reducing stage for job {self._id}")
        
        get_file_name = lambda path: path.name

        files = list(self._grouper_output_dir.glob("reduce*"))
        files.sort(key=get_file_name)
        partition = self.partition_input(files, self._num_reducers)

        job_list = [(job, None, False) for job in partition if len(job) != 0]
        job_outputs = []

        logging.info("Assigning workers for reducing")
        while len([job for job, pid, completed in job_list if not completed]) != 0:
            if self._signals["shutdown"]:
                logging.info("Shutting down in reducing stage.")
                return False, job_outputs
            for i, (job, worker_pid, completed) in enumerate(job_list):
                if completed:
                    continue
                elif worker_pid is not None and self._workers[worker_pid]["status"] == "ready":
                    # The worker has completed this job.
                    job_list[i] = (job, worker_pid, True)
                    assert(self._workers[worker_pid]["job_output"] is not None)
                    job_outputs += self._workers[worker_pid]["job_output"]
                    logging.info(f"Reducing job {i} complete")
                elif worker_pid is None:
                    for worker in self._workers.values():
                        if worker["status"] == "ready":
                            worker["status"] = "busy"
                            job_list[i] = (job, worker["pid"], False)
                            mapreduce.utils.send_message({
                                "message_type": "new_worker_job",
                                "input_files": [str(file) for file in job],
                                "executable": self._reducer_exec,
                                "output_directory": str(self._reducer_output_dir),
                                "worker_pid": worker["pid"],
                            }, worker["host"], worker["port"])
                            break
                elif self._workers[worker_pid]["status"] == "dead":
                    job_list[i] = (job, None, False)

            time.sleep(0.1)

        logging.info("Reducing stage complete.")
        return True, job_outputs
    
    def cleanup(self, output_files):
        logging.info(f"Master: Finishing job {self._id}")
        self._output_dir.mkdir(exist_ok=True)

        for file in output_files:
            copy_src = Path(file)
            copy_dst = self._output_dir/(copy_src.name).replace("reduce", "outputfile")
            shutil.copy(copy_src, copy_dst)


        self.status = "finished"

    def partition_input(self, items, num_partitions):
        # Note: Empty partitions may be returned if num_partitions > len(items).
        # In this case, it may be desirable to ignore the empty partitions.
        assert(num_partitions != 0)

        partition = [[]*1 for i in range(num_partitions)]

        i = 0
        for item in items:
            partition[i].append(item)
            i = (i + 1) % num_partitions
        
        return partition

