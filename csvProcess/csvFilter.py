#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2019 Jonathan Schultz
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from argrecord import ArgumentHelper, ArgumentRecorder
import sys
import os
import shutil
import csv
import string
import multiprocessing
import pymp
import re
from dateutil import parser as dateparser
import datetime
import calendar
import subprocess
from decimal import *
import itertools
from more_itertools import peekable

def csvFilter(arglist=None):

    parser = ArgumentRecorder(description='Multi-functional CSV file filter.',
                              fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity',  type=int, default=1, private=True)
    parser.add_argument('-j', '--jobs',       type=int, help='Number of parallel tasks, default is number of CPUs. May affect performance but not results.', private=True)
    parser.add_argument('-b', '--batch',      type=int, default=100000, help='Number of rows to process per batch. Use to limit memory usage with very large files. May affect performance but not results.', private=True)

    parser.add_argument('-p', '--prelude',    type=str, nargs="*", help='Python code to execute before processing')
    parser.add_argument('-f', '--filter',     type=str, help='Python expression evaluated to determine whether row is included')
    parser.add_argument('-c', '--column',     type=str, default='text', help='Column to apply regular expression')
    parser.add_argument('-r', '--regexp',     type=str, help='Regular expression to create output columns.')
    parser.add_argument('-i', '--ignorecase', action='store_true', help='Ignore case in regular expression')
    parser.add_argument(      '--invert',     action='store_true', help='Invert filter, that is, output those tweets that do not pass filter and/or regular expression')

    parser.add_argument(      '--since',      type=str, help='Lower bound date/time in any sensible format')
    parser.add_argument(      '--until',      type=str, help='Upper bound date/time in any sensible format')
    parser.add_argument(      '--datecol',    type=str, help='Column containing date/time date', default='date')
    parser.add_argument('-l', '--limit',      type=int, help='Limit number of rows to process')

    parser.add_argument('-C', '--copy',       action='store_true', help='If true, copy all columns from input file.')
    parser.add_argument('-x', '--exclude',    type=str, nargs="*", help='Columns to exclude from copy')
    parser.add_argument('-H', '--header',     type=str, nargs="*", help='Column names to create.')
    parser.add_argument('-d', '--data',       type=str, nargs="*", help='Python code to produce lists of values to output as columns.')

    parser.add_argument('-o', '--outfile',    type=str, help='Output CSV file, otherwise use stdout.', output=True)
    parser.add_argument(      '--rejfile',    type=str, help='Output CSV file for rejected rows')
    parser.add_argument('-n', '--number',     type=int, help='Maximum number of results to output')
    parser.add_argument('--no-comments',      action='store_true', help='Do not output descriptive comments')
    parser.add_argument('--no-header',        action='store_true', help='Do not output CSV header with column names')

    parser.add_argument('-P', '--pipe', type=str,            help='Command to pipe input from')
    parser.add_argument('infile',       type=str, nargs='?', help='Input CSV file, if neither input nor pipe is specified, stdin is used.', input=True)

    global args
    args = parser.parse_args(arglist)

    if args.jobs is None:
        args.jobs = multiprocessing.cpu_count()

    if args.verbosity >= 1:
        print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

    if args.batch is None:
        args.batch = sys.maxint

    if args.prelude:
        if args.verbosity >= 1:
            print("Executing prelude code.", file=sys.stderr)
        if args.verbosity >= 2:
            print(os.linesep.join(args.prelude), file=sys.stderr)

        exec(os.linesep.join(args.prelude), globals())

    if args.regexp:
        regexp = re.compile(args.regexp, re.IGNORECASE if args.ignorecase else 0)
        regexpfields = list(regexp.groupindex)
    else:
        regexpfields = None

    until = dateparser.parse(args.until) if args.until else None
    since = dateparser.parse(args.since) if args.since else None

    if args.infile:
        infile = open(args.infile, 'r')
    elif args.pipe:
        infile = peekable(subprocess.Popen(args.pipe, stdout=subprocess.PIPE, shell=True, text=True).stdout)
    else:
        infile = peekable(sys.stdin)

    # Read comments at start of infile.
    incomments = ArgumentHelper.read_comments(infile) or ArgumentHelper.separator()
    infieldnames = next(csv.reader([next(infile)]))
    inreader=csv.DictReader(infile, fieldnames=infieldnames)

    if args.outfile is None:
        outfile = sys.stdout
    else:
        if os.path.exists(args.outfile):
            shutil.move(args.outfile, args.outfile + '.bak')

        outfile = open(args.outfile, 'w')

    if args.rejfile:
        if os.path.exists(args.rejfile):
            shutil.move(args.rejfile, args.rejfile + '.bak')

        rejfile = open(args.rejfile, 'w')

    if not args.no_comments:
        outfile.write(parser.build_comments(args, args.outfile) + incomments)
        if args.rejfile:
            rejfile.write(parser.build_comments(args, args.rejfile) + incomments)

    if args.copy:
        if args.exclude:
            outfieldnames = [fieldname for fieldname in infieldnames if fieldname not in args.exclude]
        else:
            outfieldnames = list(infieldnames)
    else:
        outfieldnames = []

    if args.data:
        if args.header:
            #if len(args.header) != len(args.data):
                #raise RuntimeError("Number of headers must equal number of data items.")

            datafieldnames = [fieldname for fieldname in args.header]

        outfieldnames += [fieldname for fieldname in datafieldnames if fieldname not in outfieldnames]

    if regexpfields:
        outfieldnames += [fieldname for fieldname in regexpfields if fieldname not in outfieldnames]

    outcsv=csv.DictWriter(outfile, fieldnames=outfieldnames, extrasaction='ignore', lineterminator=os.linesep)

    if not args.no_header:
        outcsv.writeheader()

    if args.rejfile:
        rejcsv=csv.DictWriter(rejfile, fieldnames=outfieldnames, extrasaction='ignore', lineterminator=os.linesep)
        if not args.no_header:
            rejcsv.writeheader()

    def clean(v):
        return re.sub('\W|^(?=\d)','_', v)

    if args.filter:
        if args.verbosity >= 2:
            print("\
def evalfilter(" + ','.join([clean(fieldname) for fieldname in infieldnames]) + ",**kwargs):\n\
    return " + args.filter, file=sys.stderr)
        exec("\
def evalfilter(" + ','.join([clean(fieldname) for fieldname in infieldnames]) + ",**kwargs):\n\
    return " + args.filter, globals())

    if args.data:
        evaldatacode = "\
def evaldata(" + ','.join([clean(fieldname) for fieldname in infieldnames]) + ",**kwargs):\n"
        if len(args.data) > 1:
            evaldatacode += "\
    return (list(itertools.zip_longest(*[" + ','.join(args.data) + "])))"
        else:
            evaldatacode += "\
    return (" + args.data[0] + ")"

        if args.verbosity >= 2:
            print(evaldatacode, file=sys.stderr)
        exec(evaldatacode, globals())

    def loadrowdata(outrow, rowdata):
        if type(rowdata) == dict:
            for key, value in rowdata.items():
                outrow[key] = rowdata[key]
        elif type(rowdata) == tuple:
            for idx, value in enumerate(rowdata):
                key = datafieldnames[idx] if idx < len(datafieldnames) else str(idx)
                outrow[key] = str(value) if value is not None else ''
        elif rowdata is not None:
            key = datafieldnames[0] if 0 < len(datafieldnames) else '0'
            outrow[key] = str(rowdata)

    if args.verbosity >= 1:
        print("Loading CSV data.", file=sys.stderr)

    inrowcount = 0
    outrowcount = 0
    rejrowcount = 0
    # NB Code for single- and multi-threaded processing is separate
    if args.jobs == 1:
        for row in inreader:
            if args.limit and inrowcount == args.limit:
                break
            inrowcount += 1

            rowargs = {clean(key): value for key, value in row.items()}
            keep = True
            if args.filter:
                if args.verbosity >= 2:
                    print("evalfilter(" + repr(rowargs) + ")", file=sys.stderr)
                keep = evalfilter(**rowargs) or False
                if args.verbosity >= 2:
                    print("    --> " + repr(keep), file=sys.stderr)
            if keep and args.regexp:
                regexpmatch = regexp.match(row[args.column])
                keep = regexpmatch or False
            if keep and (since or until):
                date = row.get(args.datecol)
                if date:
                    date = dateparser.parse(date)
                    if until and date >= until:
                        keep = False
                    elif since and date < since:
                        keep = False

            if keep == args.invert and not args.rejfile:
                continue

            outrow = row.copy()
            if args.regexp and regexpmatch:
                outrow.update({regexpfield: regexpmatch.group(regexpfield) for regexpfield in regexpfields})
            if args.data:
                if args.verbosity >= 2:
                    print("evaldata(" + repr(rowargs) + ")", file=sys.stderr)
                rowdata = evaldata(**rowargs)
                if args.verbosity >= 2:
                    print("    --> " + repr(rowdata), file=sys.stderr)

                if type(rowdata) != list:
                    rowdata = [rowdata]
            else:
                rowdata = [None]

            if keep != args.invert:
                for rowdataitem in rowdata:
                    loadrowdata(outrow, rowdataitem)
                    outcsv.writerow(outrow)
                    outrowcount += 1
                    if args.number and outrowcount == args.number:
                        break
            else:
                for rowdataitem in rowdata:
                    loadrowdata(outrow, rowdataitem)
                    rejcsv.writerow(outrow)
                    rejrowcount += 1

            if args.number and outrowcount == args.number:
                break

        outfile.close()
        if args.rejfile:
            rejfile.close()
    else:
        while True:
            if args.verbosity >= 2:
                print("Loading batch.", file=sys.stderr)

            rows = []
            batchcount = 0
            while batchcount < args.batch:
                if args.limit and inrowcount == args.limit:
                    break
                try:
                    row = next(inreader)
                    inrowcount += 1
                    row['_Id'] = batchcount
                    rows.append(row)
                    batchcount += 1
                except StopIteration:
                    break

            if batchcount == 0:
                break

            if args.verbosity >= 2:
                print("Processing batch.", file=sys.stderr)

            rowcount = len(rows)
            results = pymp.shared.list()
            if args.rejfile:
                rejects = pymp.shared.list()
            with pymp.Parallel(args.jobs) as p:
                result = {}
                if args.rejfile:
                    reject = {}
                for rowindex in p.range(0, rowcount):
                    row = rows[rowindex]

                    rowargs = {clean(key): value for key, value in row.items()}
                    keep = True
                    if args.filter:
                        if args.verbosity >= 2:
                            print("evalfilter(" + repr(rowargs) + ")", file=sys.stderr)
                        keep = evalfilter(**rowargs) or False
                        if args.verbosity >= 2:
                            print("    --> " + repr(keep), file=sys.stderr)
                    if keep and args.regexp:
                        regexpmatch = regexp.match(row[args.column])
                        keep = regexpmatch or False
                    if keep and (since or until):
                        date = row.get(args.datecol)
                        if date:
                            date = dateparser.parse(date)
                            if until and date >= until:
                                keep = False
                            elif since and date < since:
                                keep = False

                    if keep == args.invert and not args.rejfile:
                        continue

                    outrow = row.copy()
                    if args.regexp and regexpmatch:
                        outrow.update({regexpfield: regexpmatch.group(regexpfield) for regexpfield in regexpfields})
                    if args.data:
                        if args.verbosity >= 2:
                            print("evaldata(" + repr(rowargs) + ")", file=sys.stderr)
                        rowdata = evaldata(**rowargs)
                        if args.verbosity >= 2:
                            print("    --> " + repr(rowdata), file=sys.stderr)
                        if type(rowdata) == list:
                            outrow['_rowdata'] = list(rowdata)
                        else:
                            loadrowdata(outrow, rowdata)
                            outrow['_rowdata'] = [None]
                    else:
                        outrow['_rowdata'] = [None]

                    if keep != args.invert:
                        result[row['_Id']] = outrow
                    else:
                        reject[row['_Id']] = outrow

                if args.verbosity >= 2:
                    print("Thread " + str(p.thread_num) + " returned " + str(len(result)) + " results.", file=sys.stderr)

                with p.lock:
                    results.append(result)
                    if args.rejfile:
                        rejects.append(reject)

            if args.verbosity >= 2:
                print("Merging batch.", file=sys.stderr)

            mergedresult = {}
            for result in results:
                mergedresult.update(result)

            if args.rejfile:
                mergedreject = {}
                for reject in rejects:
                    mergedreject.update(reject)

            if args.verbosity >= 2:
                print("Outputting batch.", file=sys.stderr)

            endindex = None
            for index in sorted(mergedresult.keys()):
                outrow = mergedresult[index]
                rowdata = outrow.get('_rowdata')
                for rowdataitem in rowdata:
                    loadrowdata(outrow, rowdataitem)
                    outcsv.writerow(mergedresult[index])
                    outrowcount += 1
                    if args.number and outrowcount == args.number:
                        break

                if args.number and outrowcount == args.number:
                    endindex = index
                    break

            if args.rejfile:
                for index in sorted(mergedreject.keys()):
                    if index < endindex:
                        break
                    outrow = mergedreject[index]
                    rowdata = outrow.get('_rowdata')
                    for rowdataitem in rowdata:
                        loadrowdata(outrow, rowdataitem)
                        rejcsv.writerow(mergedresult[index])

            if args.number and outrowcount == args.number:
                break

        outfile.close()
        if args.rejfile:
            rejfile.close()

if __name__ == '__main__':
    csvFilter(None)
