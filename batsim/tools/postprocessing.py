"""
    batsim.tools.postprocessing
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This tool may be used to postprocess experimental data for features introduced only in
    the Pybatsim sched module but not as general Batsim feature.
"""
import os

import pandas

from batsim.batsim import Batsim
from batsim.sched.events import load_events_from_file


def merge_by_parent_job(in_batsim_jobs, in_sched_events, **kwargs):
    """Function used as function in `process_jobs` to merge jobs with the same parent job id."""
    idx = 0

    result = pandas.DataFrame(
        data=None,
        columns=in_batsim_jobs.columns,
        index=in_batsim_jobs.index)
    result.drop(result.index, inplace=True)

    def add_job(*args):
        nonlocal idx
        result.loc[idx] = args
        idx += 1

    submit_events = in_sched_events.filter(types=["job_submission_received"])

    for i1, r1 in in_batsim_jobs.iterrows():
        job_id = r1["job_id"]
        workload_name = r1["workload_name"]

        full_job_id = str(
            workload_name) + Batsim.WORKLOAD_JOB_SEPARATOR + str(job_id)

        event = submit_events.filter(
            cond=lambda ev: ev.data["job"]["id"] == full_job_id).first
        job_obj = event.data["job"]

        if job_obj["parent_id"]:
            job_id = str(job_obj["parent_number"])
            workload_name = str(job_obj["parent_workload_name"])

        add_job(
            r1["allocated_processors"],
            r1["consumed_energy"],
            r1["execution_time"],
            r1["finish_time"],
            job_id,
            r1["metadata"],
            r1["requested_number_of_processors"],
            r1["requested_time"],
            r1["starting_time"],
            r1["stretch"],
            r1["submission_time"],
            r1["success"],
            r1["turnaround_time"],
            r1["waiting_time"],
            workload_name)

    return result


def process_jobs(result_prefix,
                 in_batsim_jobs, in_sched_events,
                 functions=[], float_precision=6,
                 output_separator=",",
                 verbose=False, **kwargs):
    """Tool for processing the job results.

    :param result_prefix: the prefix (including directory prefixes) for the output
                          files.

    :param in_batsim_jobs: the file of the jobs file written by Batsim

    :param in_sched_events: the file of the events file written by PyBatsim.sched

    :param functions: the functions which should be used for processing the jobs
                      and generating new data files.

    :param float_precision: the float precision for writing the output data with
                            pandas.

    :param output_separator: the field separator in the output csv file.

    :param verbose: print messages about the currently processed functions.

    :param kwargs: additional arguments forwarded to the processing functions.
    """
    result_files = []

    in_batsim_jobs_data = pandas.read_csv(in_batsim_jobs, sep=",")
    in_sched_events_data = load_events_from_file(in_sched_events)

    for f_idx, f in enumerate(functions):
        result = "{}{}.csv".format(result_prefix, f.__name__)

        try:
            os.makedirs(os.path.dirname(result))
        except FileExistsError:
            pass

        result_files.append(result)
        with open(result, 'w') as result_file:
            if verbose:
                print("[{}/{}] {}: {}, {} => {}" .format(f_idx + 1,
                                                         len(functions),
                                                         f.__name__,
                                                         in_batsim_jobs.name,
                                                         in_sched_events.name,
                                                         result))
            result_data = f(in_batsim_jobs_data, in_sched_events_data, **kwargs)

            result_data.to_csv(
                result_file,
                index=False,
                sep=output_separator,
                float_format='%.{}f'.format(float_precision))
    return result_files
