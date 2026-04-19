"""Minimal local MapReduce runner used by the homework tests."""

import glob
import os


def hadoop(input_directory, output_directory, mapper, reducer):
    """Run a local map-reduce job using the API expected by queries.py."""

    def read_records_from_input(input_folder):
        sequence = []
        path = os.path.join(input_folder, "*")
        files = glob.glob(path)
        for file in files:
            with open(file, "r", encoding="utf-8") as f:
                for line in f:
                    sequence.append((file, line))
        return sequence

    def save_results_to_output(result):
        filename = os.path.join(output_directory, "part-00000")
        with open(filename, "w", encoding="utf-8") as f:
            for key, value in result:
                f.write(f"{key}\t{value}\n")

    def create_success_file(output_folder):
        filename = os.path.join(output_folder, "_SUCCESS")
        with open(filename, "w", encoding="utf-8") as f:
            f.write("")

    def create_output_directory(output_folder):
        os.makedirs(output_folder, exist_ok=True)

    sequence = read_records_from_input(input_directory)
    pairs_sequence = mapper(sequence)
    pairs_sequence = sorted(pairs_sequence)
    result = reducer(pairs_sequence)
    create_output_directory(output_directory)
    save_results_to_output(result)
    create_success_file(output_directory)