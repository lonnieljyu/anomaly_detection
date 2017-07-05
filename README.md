# anomaly_detection

Implements the exercise described in https://github.com/InsightDataScience/anomaly_detection

# Dependencies

Bash Shell, Python 3, Pandas

# Comments

I used a variant of depth-first search called Depth-Limited DFS for the graph traversal; its time complexity is not better than DFS or BFS but its space complexity is better. Pandas DataFrames are such a great resource. Overall, this exercise was a good challenge of my software engineering skills, a good opportunity for me to learn Python 3 vs. 2, and a chance to practice with Pandas. I wish the machine learning portion was more interesting, although imagining how to make the ML part more efficient with online learning was a good problem to think about.

# Potential Improvements

I could make the streaming input calculate the mean and standard deviation more efficiently if I use Welford's algorithm for online learning of the mean and variance. There may be a more efficient way to update the social network when processing the streaming input but I couldn't think of one. I could split up the source code into multiple files for better readibility. I could add error checking for input files and unit tests.