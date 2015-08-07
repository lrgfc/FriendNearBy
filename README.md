Input:
N -- number of commands. Followed by N lines, each of the format:
Op args

Op can either be ‘Q’ or ‘W’.
Q is a query operation which takes two args:
user_id miles

miles is always one of the 3 values: 1, 5 or 30.

W is the insertion operation, which takes args of the form:
user_id lat lng interest_id1 weight1 interest_id2 weight2 ...

interest_ids are the set of all interests that user cares about. This could be his facebook friends or interests, each of them having a specific weight.

Output:

For each query operation, output a single line containing the top 10 (or less) user_id’s who are within the ‘miles’ radius of the user and have the highest scalar product of common interest weights. These must be sorted in the order of the weights highest to lowest, tie broken by user_id lowest to highest. 
