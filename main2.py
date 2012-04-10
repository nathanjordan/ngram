######### IMPORT CRAP ###############
import pymongo
from pymongo import Connection

################################################
############ FUNCTION DEFINITIONS ##############
################################################

#creates a list of tuples with a string and its count
def createGramCounts( source , f ):
	dest = []
	while len(source):
		s = source[0]
		c = source.count( s )
		removeAllFromList( s , source )
		dest.append( { "file_title": f , "gram": s , "count": c } )

	db.insert( dest )
	return dest

#removes all of item from a list
def removeAllFromList( item , l ):
	for i in range( 0 , len(l)):
		try:
			j = l.index( item )
			l.pop(j)
		except:
			break

#creates a list of ngrams from a string
def createGramsForN( words , n , f ):
	grams = []
	for i in range( 0 , len(words) - n + 1 ):
		s = ""
		for j in range( 0 , n ):
			if j == 0:
				s = s + words[i + j]
			else:
				s = s + " " + words[i +j]
		grams.append( s )
	createGramCounts( grams , f )

	return grams

def createNGrams( fileList , nList , i ):
	entityList = []

	f = open( i , "r" )
	words = f.read().split(" ")
	for j in nList:
		gramList = createGramsForN( words , j , i )

	return entityList
	

############## MAP REDUCE FUNCTIONS ##################


########## File names and number of Grams #########
fileList = [ "seuss.txt" , "macbeth.txt" ]
nList = [ 3 ]

##########################################
########### GET MONGO READY ##############
##########################################
connection = Connection()

db = connection.test.grams
db.drop()

##########################################
########### START PROGRAM ################
##########################################
for i in fileList:
	nGramDictionary = createNGrams( fileList , nList , i )

cursor = db.find()
for d in cursor:
	print d
	
############ MAP REDUCE ###############
from pymongo.code import Code

map = Code("function() {"
"this.gram.forEach(function(z) {"
" emit(z,1);"
" });"
"}")

reduce = Code("function (key, values) {"
"var total = 0;"
"for (var i = 0; i < values.length; i++) {"
" total += values[i];"
"}"
"return total;"
"}")

results = db.map_reduce(map, reduce, "myresults")

for results in results.find():
	print results

