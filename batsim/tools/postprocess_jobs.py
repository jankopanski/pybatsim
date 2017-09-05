"""
    batsim.tools.postprocess_jobs
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This command may be used to postprocess experimental data for features introduced only in
    the Pybatsim sched module but not as general Batsim feature.
"""
import click
import sys
import traceback
import pandas

from batsim.batsim import Batsim


@click.group(
    help="This is a tool to postprocess jobs for the batsim.sched package.",
    context_settings={
        'help_option_names': [
            '-h',
            '--help']})
@click.option('--debug', "-D", is_flag=True, help="Print debug messages.")
@click.pass_context
def postprocess_jobs(ctx, debug):
    ctx.obj = {}
    ctx.obj['debug'] = debug


def do_merge_jobs(in_jobs, in_sched_jobs, out_jobs, merge_subjobs=False):
    idx = 0

    def add_job(*args):
        nonlocal idx
        out_jobs.loc[idx] = args
        idx += 1

    for i1, r1 in in_jobs.iterrows():
        job_id = r1["job_id"]
        workload_name = r1["workload_name"]

        sched_job = in_sched_jobs.loc[in_sched_jobs['full_job_id'] == str(
            workload_name) + Batsim.WORKLOAD_JOB_SEPARATOR + str(job_id)].iloc[0]

        parent_workload_name = sched_job["parent_workload_name"]
        parent_job_id = sched_job["parent_job_id"]

        if merge_subjobs:
            if not pandas.isnull(parent_job_id):
                try:
                    workload_name = str(int(parent_workload_name))
                except ValueError:
                    workload_name = str(parent_workload_name)
                try:
                    job_id = str(int(parent_job_id))
                except ValueError:
                    job_id = str(parent_job_id)

        add_job(
            job_id,
            r1["hacky_job_id"],
            workload_name,
            r1["submission_time"],
            r1["requested_number_of_processors"],
            r1["requested_time"],
            r1["success"],
            r1["starting_time"],
            r1["execution_time"],
            r1["finish_time"],
            r1["waiting_time"],
            r1["turnaround_time"],
            r1["stretch"],
            r1["consumed_energy"],
            r1["allocated_processors"])


@postprocess_jobs.command("merge-jobs", help="Merge jobs from the csv file")
@click.option('--merge-subjobs', is_flag=True, help="Merge subjobs")
@click.option(
    '--float_precision',
    type=int,
    help="The float precision in the output",
    default=6)
@click.argument('in_jobs_file', type=click.File('r'))
@click.argument('in_sched_jobs_file', type=click.File('r'))
@click.argument('out_jobs_file', type=click.File('w'))
@click.pass_context
def merge_jobs(
        ctx,
        merge_subjobs,
        float_precision,
        in_jobs_file,
        in_sched_jobs_file,
        out_jobs_file):
    in_jobs = pandas.read_csv(in_jobs_file)
    in_sched_jobs = pandas.read_csv(in_sched_jobs_file, sep=";")
    out_jobs = pandas.DataFrame(
        data=None,
        columns=in_jobs.columns,
        index=in_jobs.index)
    out_jobs.drop(out_jobs.index, inplace=True)

    do_merge_jobs(
        in_jobs,
        in_sched_jobs,
        out_jobs,
        merge_subjobs=merge_subjobs)

    out_jobs.to_csv(
        out_jobs_file,
        index=False,
        sep=",",
        float_format='%.{}f'.format(float_precision))


def main():
    try:
        return postprocess_jobs(standalone_mode=True)
    except Exception as err:
        if "--debug" in sys.argv:
            click.echo("Error:", err=True)
            traceback.print_exc()
            click.echo("{}: {}".format(type(err).__name__, str(err)), err=True)
        else:
            click.echo("Error: " + str(err), err=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
