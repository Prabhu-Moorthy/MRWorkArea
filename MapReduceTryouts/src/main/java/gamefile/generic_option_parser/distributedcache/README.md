Problem:
Implementation of distributed cache through generic option parser.
Have implemented several ways to fetch the files from the distributed cache in the mapper.
I have used the Job class insetad if the depricated Distributed Cache class.

Sample Input files:
GameFile.txt
------------
11,Mario,100
11,Contra,97
11,Tank,86
22,Contra,50
22,Age of Empire,56
33,Tank,76
33,Contra,89

Age.txt
-------
11,18
22,30
33,26

hadoop jar MapReduceTryouts-1.jar gamefile.distributedcache.Driver -files game/Age.txt Input/game/GameFile.txt GameOp
