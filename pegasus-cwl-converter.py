#!/usr/bin/env python3

import logging
import argparse
import sys
import os
import shutil
from collections import namedtuple
from pprint import pformat

from yaml import Loader, load

# TODO: these need to be changed when this script is added to pegasus master
sys.path.insert(0, "/local-scratch/tanaka/pegasus/lib/pegasus/python")
#sys.path.insert(0, "/Users/ryantanaka/ISI/pegasus/dist/pegasus-5.0.0dev/lib/pegasus/python")
import Pegasus.DAX3 as dax
import cwl_utils.parser_v1_0 as cwl

log = logging.getLogger("logger")

# --- script setup -------------------------------------------------------------------
class ColoredFormatter(logging.Formatter):
    # printing debug level logs in yellow
    debug_format = "\u001b[33m%(asctime)s %(levelname)7s:  %(message)s at line %(lineno)d: \u001b[0m"

    def __init__(self):
        super().__init__("%(asctime)s %(levelname)7s:  %(message)s")

    def format(self, record):

        # save the original format
        fmt_orig = self._style._fmt

        if record.levelno == logging.DEBUG:
            self._style._fmt = ColoredFormatter.debug_format

        # Call the original formatter class to do the grunt work
        result = logging.Formatter.format(self, record)

        # revert format back to original
        self._style._fmt = fmt_orig

        return result

def setup_logger(debug_flag):
   # log to the console
   console = logging.StreamHandler()

   # default log level - make logger/console match
   log.setLevel(logging.INFO)
   console.setLevel(logging.INFO)

   # debug - from command line
   if debug_flag:
       log.setLevel(logging.DEBUG)
       console.setLevel(logging.DEBUG)

   # formatter
   console.setFormatter(ColoredFormatter())
   log.addHandler(console)
   log.debug("Logger has been configured")

def parse_args():
    parser = argparse.ArgumentParser(
            description="Converts a cwl workflow into the Pegasus DAX format.")
    parser.add_argument("cwl_workflow_file_path",
                        help="Path to the file containing the CWL Workflow class.")
    parser.add_argument("input_file_spec_path",
                        help="YAML file describing the workflow inputs.")
    parser.add_argument("output_file_path",
                        help="Desired path of the generated DAX file.")

    parser.add_argument("-d", "--debug", action="store_true",
                        dest="debug", help="Enables debugging output.")

    return parser.parse_args()

# --- utility functions --------------------------------------------------------------

def get_basename(name):
    basename = name.split("#")[1]
    return basename

def get_name(namespace_id, field_id):
    return get_basename(namespace_id) + "/" + get_basename(field_id)

# --- pegasus catalog classes --------------------------------------------------------

class ReplicaCatalog:
    # TODO: make this more comprehensive
    # TODO: possibly make a catalog type, as the behavior here is almost the
    #       same as ReplicaCatalog
    def __init__(self):
        self.entries = set()

    # TODO: account for more complex entries into the catalog
    def add_item(self, lfn, pfn, site):
        entry = "{0} file://{1} site={2}".format(lfn, pfn, site)
        log.debug("Adding to RC: '{}'".format(entry))
        self.entries.add(entry)

    def write_catalog(self, filename):
        log.info("Writing replica catalog to {}".format(filename))
        with open(filename, "w") as rc:
            for entry in self.entries:
                rc.write(entry + "\n")


class TransformationCatalog:

    Entry = namedtuple("Entry", ["base_command", "is_stageable", "site", "pfn"])

    # TODO: make this more comprehensive
    def __init__(self, stageables_directory):
        self.entries = set()
        self.stageables_directory = stageables_directory

    # TODO: account for more complex entries into the catalog
    # TODO: this will need to write to YAML file in 5.0
    # TODO: if pfn is relative, then assume stageable,
    #       else assume it isn't. Need to see if there is a
    #       field in command line tool that specifies
    #       whether or not this tool is stageable (possibly place this
    #       information in a separate yaml file)
    def add_item(self, base_command, pfn):
        is_stageable = not os.path.isabs(pfn)

        # TODO: site may need to be specified in another yaml file 
        site = ""

        if is_stageable:
            log.warning(("CommandLineTool.baseName: '{}', is an absolute path, "
                        "assuming to be NOT stageable.").format(base_command))

            site = "local"
            pfn = "file://" + os.path.join(
                        os.path.abspath(self.stageables_directory), 
                        pfn
                     )

        else:
            site = "condorpool"
            log.warning(("CommandLineTool.baseName: '{}', is not an absolute path, "
                        "assuming to be stageable.").format(base_command))


        entry = TransformationCatalog.Entry(base_command, is_stageable, site, pfn)
        log.debug("Adding to TC: {}".format(entry))
        self.entries.add(entry)

    def write_catalog(self, filename):
        log.info("Writing tranformation catalog to {}".format(filename))
        with open(filename, "w") as tc:
            for entry in self.entries:
                tc.write("tr {} {{\n".format(entry.base_command))
                # TODO: handle different sites
                tc.write("    site {} {{\n".format(entry.site))
                tc.write("        pfn \"{}\"\n".format(entry.pfn))
                if entry.is_stageable:
                    tc.write("        type \"STAGEABLE\"\n")
                tc.write("    }\n")
                tc.write("}\n")
# --- cwl -> dax conversion  ---------------------------------------------------------

def main():
    args = parse_args()
    setup_logger(args.debug)

    workflow_file_path = args.cwl_workflow_file_path
    workflow_file_dir = os.path.dirname(workflow_file_path)

    log.info("Loading {}".format(workflow_file_path))
    workflow = cwl.load_document(workflow_file_path)

    adag = dax.ADAG("dag-generated-from-cwl", auto=True)
    rc = ReplicaCatalog()
    tc = TransformationCatalog(workflow_file_dir)

    # process initial input file(s)
    # TODO: need to account for the different fields for a file class
    # TODO: log warning for the fields that we are skipping
    workflow_input_strings = dict()
    workflow_files = dict()

    log.info("Collecting inputs in {}".format(args.input_file_spec_path))
    with open(args.input_file_spec_path, "r") as yaml_file:
        input_file_specs = load(yaml_file, Loader=Loader)

        for input in workflow.inputs:
            input_type = input.type

            if input_type == "File":
                workflow_files[get_basename(input.id)] = get_basename(input.id)
                # TODO: account for non-local sites
                rc.add_item(get_basename(input.id), input_file_specs[get_basename(input.id)]["path"], "local")
            elif input_type == "string":
                workflow_input_strings[get_basename(input.id)] = \
                                        input_file_specs[get_basename(input.id)]
            elif isinstance(input_type, cwl.InputArraySchema):
                if input_type.items == "File":
                    # TODO: account for workflow inputs of type File[]
                    pass
                elif input_type.items == "string":
                    workflow_input_strings[get_basename(input.id)] = \
                                        input_file_specs[get_basename(input.id)]

    log.info("Collecting output files")
    for step in workflow.steps:
        cwl_command_line_tool = cwl.load_document(step.run) if isinstance(step.run, str) \
                                                                    else step.run

        for output in cwl_command_line_tool.outputs:
            # TODO: account for outputs that are not files
            output_name = get_name(step.id, output.id)

            log.debug("Adding (key: {0}, value: {1}) to workflow_files".format(
                output_name,
                output.outputBinding.glob
            ))

            # TODO: throw error when glob contains javascript expression
            #       or pattern as we cannot support anything that is dynamic
            workflow_files[output_name] = output.outputBinding.glob

    log.info("Building workflow steps into dax jobs")
    for step in workflow.steps:
        # convert cwl:CommandLineTool -> pegasus:Executable
        cwl_command_line_tool = cwl.load_document(step.run) if isinstance(step.run, str) \
                                                                    else step.run

        executable_name = os.path.basename(cwl_command_line_tool.baseCommand) if \
            os.path.isabs(cwl_command_line_tool.baseCommand) else cwl_command_line_tool.baseCommand

        dax_executable = dax.Executable(executable_name)

        # add executable to transformation catalog
        tc.add_item(executable_name, cwl_command_line_tool.baseCommand)

        # create job with executable
        dax_job = dax.Job(dax_executable)

        step_inputs = dict()
        for input in step.in_:
            input_id = get_basename(input.id)
            if isinstance(input.source, str):
                step_inputs[input_id] = get_basename(input.source)
            elif isinstance(input.source, list):
                step_inputs[input_id] = [get_basename(file) for file in input.source]

        # add input uses to job
        for input in cwl_command_line_tool.inputs:
            if input.type == "File":
                file_id = step_inputs[get_name(step.id, input.id)]
                file = dax.File(workflow_files[file_id])
                log.debug("Adding link ({0} -> {1})".format(
                                file_id,
                                dax_job.name
                                )
                )

                dax_job.uses(
                    file,
                    link=dax.Link.INPUT
                )

            # TODO: better type checking for string[] and File[] ?
            elif isinstance(input.type, cwl.CommandInputArraySchema):
                if input.type.items == "File":
                    file_ids = step_inputs[get_name(step.id, input.id)]
                    for file_id in file_ids:
                        file = dax.File(workflow_files[file_id])
                        log.debug("Adding link ({0} -> {1})".format(
                                        file_id,
                                        dax_job.name
                                        )
                        )

                        dax_job.uses(
                            file,
                            link=dax.Link.INPUT
                        )


        # add output uses to job
        # TODO: ensure that these are of type File or File[]
        for output in step.out:
            file_id = get_basename(output)
            file = dax.File(workflow_files[file_id])
            log.debug("Adding link ({0} -> {1})".format(
                dax_job.name,
                file_id
            ))

            dax_job.uses(
                    file,
                    link=dax.Link.OUTPUT,
                    transfer=True,
                    register=True
                )

        # add arguments to job
        # TODO: place argument building up in a function
        dax_job_args = cwl_command_line_tool.arguments if \
            cwl_command_line_tool.arguments is not None else []

        # process cwl inputBindings if they exist and build up job argument list
        cwl_command_line_tool_inputs = sorted(cwl_command_line_tool.inputs,
            key=lambda input : input.inputBinding.position if input.inputBinding.position \
                is not None else 0 )

        for input in cwl_command_line_tool_inputs:
            # process args
            if input.inputBinding is not None:
                # TODO: account for inputBinding separation
                if input.inputBinding.prefix is not None:
                    dax_job_args.append(input.inputBinding.prefix)

                if input.type == "File":
                    dax_job_args.append(dax.File(
                            workflow_files[step_inputs[get_name(step.id, input.id)]]
                        )
                    )

                if input.type == "string":
                    dax_job_args.append(workflow_input_strings[
                            step_inputs[get_name(step.id, input.id)]
                        ]
                    )

                # handle array type inputs
                if isinstance(input.type, cwl.CommandInputArraySchema):
                    if input.type.items == "File":
                        for file in step_inputs[get_name(step.id, input.id)]:
                            dax_job_args.append(dax.File(workflow_files[file]))
                    elif input.type.items == "string":
                        input_string_arr_id = step_inputs[get_name(step.id, input.id)]

                        separator = " " if input.inputBinding.itemSeparator is None \
                                        else input.inputBinding.itemSeparator

                        dax_job_args.append(
                            # TODO: currently only accounting for input strings that
                            #       are inputs to the entire workflow
                            separator.join(workflow_input_strings[
                                input_string_arr_id
                            ])
                        )

        log.debug("Adding job: {0}, with args: {1}".format(
            dax_job.name,
            dax_job_args
        ))
        dax_job.addArguments(*dax_job_args)

        # add job to DAG
        adag.addJob(dax_job)

    '''
    # TODO: fix this, can't have forward slash in lfn, so replacing
    # with "." for now to get this working
    for filename, file in adag.files.items():
        if "/" in filename:
            file.name.replace("/", ".")

    # TODO: fix this, can't have forward slash in lfn, so replacing
    # with "." for now to get this working
    for jobid, job in adag.jobs.items():
        for used in job.used:
            if "/" in used.name:
                used.name = used.name.replace("/", ".")

        for arg in job.arguments:
            if isinstance(arg, dax.File):
                if "/" in arg.name:
                    arg.name = arg.name.replace("/", ".")
    '''
    rc.write_catalog("rc.txt")
    tc.write_catalog("tc.txt")

    with open(args.output_file_path, "w") as f:
        log.info("Writing DAX to {}".format(args.output_file_path))
        adag.writeXML(f)

if __name__=="__main__":
    sys.exit(main())
