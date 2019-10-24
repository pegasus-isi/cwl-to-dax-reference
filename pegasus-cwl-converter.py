#!/usr/bin/env python3

import logging
import argparse
import sys
import os
import shutil
from pprint import pformat

from yaml import Loader, load

# TODO: these need to be changed when this script is added to pegasus master
#sys.path.insert(0, "/nfs/u2/tanaka/pegasus/lib/pegasus/python")
sys.path.insert(0, "/Users/ryantanaka/ISI/pegasus/dist/pegasus-5.0.0dev/lib/pegasus/python")
import Pegasus.DAX3 as dax
import cwl_utils.parser_v1_0 as cwl

log = logging.getLogger("logger")

# --- script setup -------------------------------------------------------------------
class ColoredFormatter(logging.Formatter):
    # printing debug level logs in yellow 
    debug_format = "\u001b[33m %(asctime)s %(levelname)7s:  %(message)s at line %(lineno)d: \u001b[0m"
    info_format = "%(asctime)s %(levelname)7s:  %(message)s"

    def __init__(self):
        super().__init__("%(asctime)s %(levelname)7s:  %(message)s")

    def format(self, record):
        if record.levelno == logging.DEBUG:
            self._style._fmt = ColoredFormatter.debug_format
        elif record.levelno == logging.INFO:
            self._style._fmt = ColoredFormatter.info_format

        return logging.Formatter.format(self, record)

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
        self.items = set()

    # TODO: account for more complex entries into the catalog
    def add_item(self, lfn, pfn, site):
        entry = "{0} {1} site={2}".format(lfn, pfn, site)
        log.debug("Adding to RC: '{}'".format(entry))
        self.items.add(entry)

    def write_catalog(self, filename):
        log.info("Writing replica catalog to {}".format(filename))
        with open(filename, "w") as rc:
            for item in self.items:
                rc.write(item + "\n")


class TransformationCatalog:
    # TODO: make this more comprehensive
    def __init__(self):
        self.items = set()

    # TODO: account for more complex entries into the catalog
    def add_item(self, name, pfn):
        entry = (name, pfn)
        log.debug("Adding to TC: {}".format(entry))
        self.items.add(entry)

    def write_catalog(self, filename):
        log.info("Writing tranformation catalog to {}".format(filename))
        with open(filename, "w") as tc:
            for item in self.items:
                tc.write("tr {} {{\n".format(item[0]))
                tc.write("    site condorpool {\n")
                tc.write("        pfn \"{}\"\n".format(item[1]))
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
    tc = TransformationCatalog()

    # process initial input file(s)
    # TODO: need to account for the different fields for a file class
    # TODO: log warning for the fields that we are skipping
    workflow_input_strings = dict()
    workflow_files = dict()

    log.info("Collecting inputs in {}".format(args.input_file_spec_path))
    with open(args.input_file_spec_path, "r") as yaml_file:
        input_file_specs = load(yaml_file, Loader=Loader)
        for id, fields in input_file_specs.items():
            # TODO: account for types File[] and string[]
            if isinstance(fields, dict):
                if fields["class"] == "File":
                    workflow_files[id] = id
                    rc.add_item(id, fields["path"], "local")
            elif isinstance(fields, str):
                workflow_input_strings[id] = fields


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

        dax_executable = dax.Executable(cwl_command_line_tool.baseCommand)

        # add executable to transformation catalog
        tc.add_item(cwl_command_line_tool.baseCommand, cwl_command_line_tool.baseCommand)

        # create job with executable
        dax_job = dax.Job(dax_executable)

        # get the inputs of this step
        step_inputs = {get_basename(input.id) : get_basename(input.source) \
                                                            for input in step.in_}

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

                # TODO: take into account string inputs that are outputs of other steps
                #       and not just workflow inputs

                input_string_id = step_inputs[get_name(step.id, input.id)]

                arg_string = ""
                if input.type == "string[]":
                    separator = " " if input.inputBinding.itemSeparator is None \
                                        else input.inputBinding.itemSeparator

                    arg_string += separator.join(
                        workflow_input_strings[input_string_id]
                    )
                elif input.type == "string":
                    arg_string += workflow_input_strings[input_string_id]

                dax_job_args.append(arg_string)

        log.debug("Adding job: {0}, with args: {1}".format(
            dax_job.name,
            dax_job_args
        ))
        dax_job.addArguments(*dax_job_args)

        # add job to DAG
        adag.addJob(dax_job)

    rc.write_catalog("rc.txt")
    tc.write_catalog("tc.txt")

    with open(args.output_file_path, "w") as f:
        log.info("Writing DAX to {}".format(args.output_file_path))
        adag.writeXML(f)

if __name__=="__main__":
    sys.exit(main())
