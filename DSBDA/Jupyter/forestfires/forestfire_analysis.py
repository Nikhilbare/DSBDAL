from mrjob.job import MRJob
from mrjob.step import MRStep

class ForestFireAnalysis(MRJob):

    def mapper(self, _, line):
        # Split the line into fields
        fields = line.strip().split(',')
        # Extract month and area
        if len(fields) == 13:
            month = fields[2]
            area = float(fields[12])
            yield month, area

    def reducer(self, month, areas):
        # Sum up the areas for each month
        total_area = sum(areas)
        yield month, total_area

if __name__ == '__main__':
    # Run the job in local mode within Jupyter Notebook
    ForestFireAnalysis.run()
