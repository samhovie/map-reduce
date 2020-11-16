"""Map-reduce job execution."""
import heapq
import itertools
import logging
import time
import shutil
from pathlib import Path
import mapreduce.utils


class Job:
    """A map-reduce job, with mapping, grouping, and reducing stages."""

    _next_id = 0

    def __init__(
            self,
            job_params,
            workers,
            signals):
        """Create a Job."""
        self._status = "waiting"
        self._job_params = job_params
        self._workers = workers
        self._signals = signals

        self._id = Job._next_id
        Job._next_id += 1

        logging.info(
            "Master: Received job %d: %s %s %s %s %d %d",
            self._id,
            self._job_params["input_dir"],
            self._job_params["output_dir"],
            self._job_params["mapper_exec"],
            self._job_params["reducer_exec"],
            self._job_params["num_mappers"],
            self._job_params["num_reducers"]
        )

        tmp_folder = Path("tmp")/f"job-{self._id}"
        tmp_folder.mkdir()

        self._tmp_output_dirs = {
            "mapper": tmp_folder/"mapper-output",
            "grouper": tmp_folder/"grouper-output",
            "reducer": tmp_folder/"reducer-output",
        }

        self._tmp_output_dirs["mapper"].mkdir()
        self._tmp_output_dirs["grouper"].mkdir()
        self._tmp_output_dirs["reducer"].mkdir()

    def start(self):
        """Run this job to completion."""
        logging.info("Master: Starting job %s", self._id)

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
        """Run the mapping stage for this job to completion."""
        logging.info("Master: Starting mapping stage for job %s", self._id)

        files = list(self._job_params["input_dir"].glob("*"))
        files.sort(key=lambda path: path.name)
        partition = mapreduce.utils.partition_input(
            files,
            self._job_params["_num_mappers"]
        )

        # Each job is a tuple containing the list of input files, the PID of
        # the worker assigned to the job, and whether the job is finished.
        job_list = [(job, None, False) for job in partition if len(job) != 0]
        job_outputs = []

        logging.info("Assigning workers for mapping")

        def on_available(worker, job, _):
            mapreduce.utils.send_message({
                "message_type": "new_worker_job",
                "input_files": [str(file) for file in job],
                "executable": self._job_params["mapper_exec"],
                "output_directory": str(
                    self._tmp_output_dirs["mapping"]
                ),
                "worker_pid": worker["pid"],
            }, worker["host"], worker["port"])

        completed, job_outputs = self.round_robin_assign(
            "mapping",
            job_list,
            on_available
        )
        if not completed:
            return False, job_outputs
        logging.info("Mapping stage complete.")
        logging.info("%s outputs from mapping", len(job_outputs))
        return True, job_outputs

    def grouping(self, output_files):
        """Run the grouping stage for this job to completion."""
        logging.info("Master: Starting grouping stage for job %s", self._id)

        output_files.sort()
        ready_workers = [
            worker for worker in self._workers.values()
            if worker["status"] == "ready"
        ]
        partition = mapreduce.utils.partition_input(
            output_files,
            len(ready_workers)
        )

        # Each job is a tuple containing the list of input files, the PID of
        # the worker assigned to the job, and whether the job is finished.
        job_list = [(job, None, False) for job in partition if len(job) != 0]

        logging.info("Assigning workers for grouping")

        def on_available(worker, job, i):
            file_name = f"sorted{(i + 1):02}"
            out_file = self._tmp_output_dirs["grouper"]/file_name
            mapreduce.utils.send_message({
                "message_type": "new_sort_job",
                "input_files": [str(file) for file in job],
                "output_file": str(out_file),
                "worker_pid": worker["pid"],
            }, worker["host"], worker["port"])

        completed, job_outputs = self.round_robin_assign(
            "grouping",
            job_list,
            on_available
        )

        if not completed:
            return False

        self.group(job_outputs)

        logging.info("Grouping stage complete.")
        return True

    def group(self, sorted_outputs):
        """Group the files in sorted_outputs."""
        files = [Path(file).open("r") for file in sorted_outputs]
        sorted_lines = heapq.merge(*files)

        # Open reducer files
        reducer_files = []
        for i in range(self._job_params["num_reducers"]):
            reducer_files.append(
                Path(
                    self._tmp_output_dirs["grouper"]/f"reduce{(i + 1):02}"
                ).open("w")
            )

        i = 0
        grouped_lines = itertools.groupby(
            sorted_lines,
            key=lambda line: line.split("\t")[0]
        )
        for _, group in grouped_lines:
            for line in group:
                reducer_files[i].writelines([line])
            i = (i + 1) % (self._job_params["num_reducers"])

        for file in reducer_files:
            file.close()

        for file in files:
            file.close()

    def reducing(self):
        """Run the reducing stage for this job to completion."""
        logging.info("Master: Starting reducing stage for job %d", self._id)

        files = list(self._tmp_output_dirs["grouper"].glob("reduce*"))
        files.sort(key=lambda path: path.name)
        partition = mapreduce.utils.partition_input(
            files,
            self._job_params["num_reducers"]
        )

        job_list = [(job, None, False) for job in partition if len(job) != 0]
        job_outputs = []

        def on_available(worker, job, _):
            mapreduce.utils.send_message({
                "message_type": "new_worker_job",
                "input_files": [str(file) for file in job],
                "executable": self._job_params["reducer_exec"],
                "output_directory": str(
                    self._tmp_output_dirs["reducer"]
                ),
                "worker_pid": worker["pid"],
            }, worker["host"], worker["port"])

        logging.info("Assigning workers for reducing")

        completed, job_outputs = self.round_robin_assign(
            "reducing",
            job_list,
            on_available
        )

        if not completed:
            return False, job_outputs

        logging.info("Reducing stage complete.")
        return True, job_outputs

    def cleanup(self, output_files):
        """Copy the output from the reducing stage to the output directory."""
        logging.info("Master: Finishing job %s", self._id)
        self._job_params["output_dir"].mkdir(exist_ok=True)

        for file in output_files:
            copy_src = Path(file)
            copy_dst = self._job_params["output_dir"]/(copy_src.name).replace(
                "reduce",
                "outputfile"
            )
            shutil.copy(copy_src, copy_dst)

        self._status = "finished"

    def round_robin_assign(self, stage, job_list, on_available):
        """Assign workers to perform the jobs in job_list."""
        job_outputs = []
        while len([job for job, _, done in job_list if not done]) != 0:
            if self._signals["shutdown"]:
                logging.info("Shutting down in %s stage.", stage)
                return False, job_outputs
            for i, (job, worker_pid, completed) in enumerate(job_list):
                if completed:
                    continue
                if (worker_pid is not None and
                        self._workers[worker_pid]["status"] == "ready"):
                    # The worker has completed this job.
                    job_list[i] = (job, worker_pid, True)
                    assert self._workers[worker_pid]["job_output"] is not None
                    job_outputs += self._workers[worker_pid]["job_output"]
                    logging.info("%s job %d complete", stage, i)
                    logging.info(
                        "%s outputs from job",
                        len(self._workers[worker_pid]["job_output"])
                    )
                elif worker_pid is None:
                    for worker in self._workers.values():
                        if worker["status"] == "ready":
                            worker["status"] = "busy"
                            job_list[i] = (job, worker["pid"], False)
                            on_available(worker, job, i)
                            break
                elif self._workers[worker_pid]["status"] == "dead":
                    job_list[i] = (job, None, False)
            time.sleep(0.1)
        return True, job_list
