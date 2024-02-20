import argparse
import logging 
import sys 
from pyflink.common import Row 
from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes, FormatDescriptor)
from pyflink.table.expressions import lit, col 
from pyflink.table.udf import udtf

data = [
    "It was the best of times, it was the worst of times,",
    "it was the age of wisdom, it was the age of foolishness,",
    "it was the epoch of belief, it was the epoch of incredulity,",
    "it was the season of Light, it was the season of Darkness,",
    "it was the spring of hope, it was the winter of despair,",
    "we had everything before us, we had nothing before us,",
    "we were all going direct to Heaven, we were all going direct the other way--",
    "in short, the period was so far like the present period,",
    "that some of its noisiest authorities insisted on its being received,",
    "for good or for evil, in the superlative degree of comparison only.",
    "There were a king with a large jaw and a queen with a plain face,",
    "on the throne of England; there were a king with a large jaw and a queen with",
    "a fair face, on the throne of France.",
    "In both countries it was clearer than crystal to the lords of the State",
    "preserves of loaves and fishes that things in general were settled for ever."
]

def word_count(input_path, output_path): 
    env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    env.get_config().set("parallelism.default", "1")
    if input_path is not None: 
        env.create_temporary_table(
            'source', TableDescriptor.for_connector('filesystem').Schema(Schema.new_builder().column('word', DataTypes.STRING()).build())
            .option('path', input_path).format('csv').build()
        )
        table = env.from_path('source')
    else: 
        print("Executing word count with default dataset.")
        print("Use --input to specify input path")
        table = env.from_elements(map(lambda i: (i,), data), DataTypes.ROW([DataTypes.FIELD('line', DataTypes.STRING())]))
    if output_path is not None: 
        env.create_temporary_table(
            'sink', TableDescriptor.for_connector('filesystem').schema(Schema.new_builder()
            .column('word', DataTypes.STRING())
            .column('count', DataTypes.BIGINT())
            .build())
            .option('path', output_path)
            .format(FormatDescriptor.for_format('canal-json').build())
            .build()
        )
    else: 
        print("Printing result to stdout. Use --output to specify output path")
        env.create_temporary_table(
            'sink', TableDescriptor.for_connector('print').schema(Schema.new_builder()
            .column('word', DataTypes.STRING())
            .column('count', DataTypes.BIGINT())
            .build()).build()
        )

    @udtf(result_types=[DataTypes.STRING()])
    def split(line: Row): 
        for s in line[0].split(): 
            yield Row(s)
    table.flat_map(split).alias('word').group_by(col('word')).select(col('word'), lit(1).count).execute_insert('sink').wait()

if __name__ == "__main__": 
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=False, help='Input file')
    parser.add_argument('--output', dest='output', required=False, help='Output file')
    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)
    word_count(known_args.input, known_args.output)