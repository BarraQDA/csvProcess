#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2016 Jonathan Schultz
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

from __future__ import print_function
import argparse
import sys
import os
import codecs
import unicodecsv
import string
import re
from dateutil import parser as dateparser
import pymp
from wordcloud import WordCloud

def csvCloud(arglist):
    parser = argparse.ArgumentParser(description='Twitter feed word cloud.',
                                     fromfile_prefix_chars='@')

    parser.add_argument('-v', '--verbosity', type=int, default=1)
    parser.add_argument('-j', '--jobs',      type=int, help='Number of parallel tasks, default is number of CPUs')
    parser.add_argument('-b', '--batch',     type=int, default=100000, help='Number of tweets to process per batch. Use to limit memory usage with very large files. May affect performance but not results.')

    parser.add_argument('-p', '--prelude',   type=str, nargs="*", help='Python code to execute before processing')
    parser.add_argument('-f', '--filter',    type=str, help='Python expression evaluated to determine whether tweet is included')
    parser.add_argument(      '--since',     type=str, help='Lower bound date/time in any sensible format')
    parser.add_argument(      '--until',     type=str, help='Upper bound date/time in any sensible format')
    parser.add_argument('-l', '--limit',     type=int, help='Limit number of tweets to process')

    parser.add_argument('-c', '--column',    type=str, default='text', help='Text column')
    parser.add_argument('-s', '--score',     type=str,                 help='Comma separated list of score columns')
    parser.add_argument('-x', '--exclude',   type=lambda s: unicode(s, 'utf8'), help='Comma separated list of words to exclude from cloud')

    parser.add_argument('-m', '--mode',      choices=['word', 'lemma', 'phrase'], default='word')

    # Arguments to pass to wordcloud
    parser.add_argument('--max_font_size', type=int)
    parser.add_argument('--max_words',     type=int)
    parser.add_argument('--width',         type=int, default=600)
    parser.add_argument('--height',        type=int, default=800)
    parser.add_argument('-o', '--outfile', type=str, help='Output image file, otherwise display on screen.')

    parser.add_argument('infile', type=str, nargs='?', help='Input CSV file, if missing use stdin.')

    parser.add_argument('--no-comments',     action='store_true',
                                                    help='Do not produce a comments logfile')

    args = parser.parse_args(arglist)
    hiddenargs = ['verbosity', 'jobs', 'batch', 'no_comments']

    if args.jobs is None:
        import multiprocessing
        args.jobs = multiprocessing.cpu_count()

    if args.verbosity >= 1:
        print("Using " + str(args.jobs) + " jobs.", file=sys.stderr)

    if args.batch == 0:
        args.batch = sys.maxint

    if args.prelude:
        if args.verbosity >= 1:
            print("Executing prelude code.", file=sys.stderr)

        exec(os.linesep.join(args.prelude)) in globals()

    until = dateparser.parse(args.until) if args.until else None
    since = dateparser.parse(args.since) if args.since else None

    if args.infile is None:
        infile = sys.stdin
    else:
        infile = open(args.infile, 'rU')

    # Skip comments at start of infile.
    incomments = ''
    while True:
        line = infile.readline()
        if line[:1] == '#':
            incomments += line
        else:
            infieldnames = next(unicodecsv.reader([line]))
            break

    if args.outfile and not args.no_comments:
        comments = (' ' + args.outfile + ' ').center(80, '#') + '\n'
        comments += '# ' + os.path.basename(sys.argv[0]) + '\n'
        arglist = args.__dict__.keys()
        for arg in arglist:
            if arg not in hiddenargs:
                val = getattr(args, arg)
                if type(val) == str or type(val) == unicode:
                    comments += '#     --' + arg + '="' + val + '"\n'
                elif type(val) == bool:
                    if val:
                        comments += '#     --' + arg + '\n'
                elif type(val) == list:
                    for valitem in val:
                        if type(valitem) == str:
                            comments += '#     --' + arg + '="' + valitem + '"\n'
                        else:
                            comments += '#     --' + arg + '=' + str(valitem) + '\n'
                elif val is not None:
                    comments += '#     --' + arg + '=' + str(val) + '\n'

        logfilename = args.outfile.rsplit('.',1)[0] + '.log'
        logfile = codecs.open(logfilename, 'w', 'utf-8')
        logfile.write(comments)
        logfile.write(incomments)
        logfile.close()

    inreader=unicodecsv.DictReader(infile, fieldnames=infieldnames)

    argbadchars = re.compile(r'[^0-9a-zA-Z_]')
    if args.filter:
        exec "\
def evalfilter(" + ','.join([argbadchars.sub('_', fieldname) for fieldname in twitterread.fieldnames]) + ",**kwargs):\n\
    return " + args.filter in locals()

    from nltk.corpus import stopwords
    exclude = set(stopwords.words('english'))
    if args.exclude is not None:
        exclude = exclude.union(word.lower() for word in args.exclude.split(','))

    score = args.score.split(',') if args.score else None

    if args.mode == 'lemma':
        from nltk import word_tokenize, pos_tag
        from nltk.corpus import wordnet
        from nltk.stem.wordnet import WordNetLemmatizer

        lemmatizer = WordNetLemmatizer()

    if args.verbosity >= 1:
        print("Loading CSV data.", file=sys.stderr)

    inrowcount = 0
    mergedscoredicts = {}

    # NB Code for single- and multi-threaded processing is separate
    if args.jobs == 1:
        for row in inreader:
            if args.limit and inrowcount == args.limit:
                break
            inrowcount += 1

            rowargs = {argbadchars.sub('_', key): value for key, value in row.iteritems()}
            keep = True
            if args.filter:
                keep = evalfilter(**rowargs) or False
            if keep and (since or until):
                date = row.get('date')
                if date:
                    date = dateparser.parse(date)
                    if until and date >= until:
                        keep = False
                    elif since and date < since:
                        keep = False

            if not keep:
                continue

            text = row[args.column]
            if args.mode == 'lemma':
                wordlist = []
                words = pos_tag(word_tokenize(text))
                for word in words:
                    if word[1].startswith('J'):
                        pos = wordnet.ADJ
                    elif word[1].startswith('V'):
                        pos = wordnet.VERB
                    elif word[1].startswith('N'):
                        pos = wordnet.NOUN
                    elif word[1].startswith('R'):
                        pos = wordnet.ADV
                    else:
                        pos = None

                    if pos:
                        lemma = lemmatizer.lemmatize(word[0], pos=pos)
                        if lemma.lower() not in exclude:
                            wordlist += [lemma]
            elif args.mode == 'word':
                wordlist = [word for word in text.split() if word.lower() not in exclude]
            else:
                wordlist = [text]

            for word in wordlist:
                if score is None:
                    wordscore = 1
                else:
                    wordscore = 0
                    for col in score:
                        wordscore += int(row[col])

                mergedscoredicts[word] = mergedscoredicts.get(word, 0) + wordscore

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
                    rows.append(next(inreader))
                    inrowcount += 1
                    batchcount += 1
                except StopIteration:
                    break

            if batchcount == 0:
                break

            if args.verbosity >= 2:
                print("Processing batch.", file=sys.stderr)

            rowcount = len(rows)
            scoredicts = pymp.shared.list()
            with pymp.Parallel(args.jobs) as p:
                scoredict = {}
                for rowindex in p.range(0, rowcount):
                    row = rows[rowindex]

                    rowargs = {argbadchars.sub('_', key): value for key, value in row.iteritems()}
                    keep = True
                    if args.filter:
                        keep = evalfilter(**rowargs) or False
                    if keep and (since or until):
                        date = row.get('date')
                        if date:
                            date = dateparser.parse(date)
                            if until and date >= until:
                                keep = False
                            elif since and date < since:
                                keep = False

                    if not keep:
                        continue

                    text = row[args.column]
                    if args.mode == 'lemma':
                        if args.mode == 'lemma':
                            wordlist = []
                            words = pos_tag(word_tokenize(text))
                            for word in words:

                                if word[1].startswith('J'):
                                    pos = wordnet.ADJ
                                elif word[1].startswith('V'):
                                    pos = wordnet.VERB
                                elif word[1].startswith('N'):
                                    pos = wordnet.NOUN
                                elif word[1].startswith('R'):
                                    pos = wordnet.ADV
                                else:
                                    pos = None

                                if pos:
                                    lemma = lemmatizer.lemmatize(word[0], pos=pos)
                                    if lemma.lower() not in exclude:
                                        wordlist += [lemma]
                    elif args.mode == 'word':
                        wordlist = [word for word in text.split() if word.lower() not in exclude]
                    else:
                        wordlist = [text]

                    for word in wordlist:
                        if score is None:
                            wordscore = 1
                        else:
                            wordscore = 0
                            for col in score:
                                wordscore += int(row[col])

                        scoredict[word] = scoredict.get(word, 0) + wordscore

                with p.lock:
                    scoredicts += [scoredict]

            for scoredict in scoredicts:
                for index in scoredict:
                    mergedscoredicts[index] = mergedscoredicts.get(index, 0) + scoredict[index]

    if args.verbosity >= 1:
        print("Generating word cloud.", file=sys.stderr)

    # Generate a word cloud image
    wordcloud = WordCloud(max_font_size=args.max_font_size,
                        max_words=args.max_words,
                        width=args.width,
                        height=args.height).generate_from_frequencies(mergedscoredicts)

    if args.outfile:
        wordcloud.to_file(args.outfile)
    else:
        # Display the generated image:
        # the matplotlib way:
        import matplotlib.pyplot as plt
        plt.figure()
        plt.imshow(wordcloud)
        plt.axis("off")
        plt.show()

        # The pil way (if you don't have matplotlib)
        #image = wordcloud.to_image()
        #image.show()

if __name__ == '__main__':
    csvCloud(None)
