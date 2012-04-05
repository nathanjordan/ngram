import pymongo

################################################################
###############    Function Definitions    #####################
################################################################

#creates a list of tuples with a string and its count
def createGramCounts( source ):
	dest = []
	while len(source):
		s = source[0]
		c = source.count( s )
		removeAllFromList( s , source )
		dest.append( { "gram" : s , "count" : c } )
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
def createGramsForN( words , n ):
	grams = []
	for i in range( 0 , len(words) - n + 1 ):
		s = ""
		for j in range( 0 , n ):
			if j == 0:
				s = s + words[i + j]
			else:
				s = s + " " + words[i + j]
		grams.append( s )
	return grams

def createNGrams( fileList , nList ):
	entityList = []
	for i in fileList:
		f = open( i , "r" )
		words = f.read().split(" ")
		gramEntity = { "filename" : i }
		for j in nList:
			gramList = createGramsForN( words , j )
			gramListWithCounts = createGramCounts( gramList )
			gramEntity[ "grams" + str(j) ] = gramListWithCounts
		entityList.append( gramEntity )
	return entityList
	
################################################################
######################    Program    ###########################
################################################################

##### NGram stuff ##############################################

fileList = [ "seuss.txt" , "macbeth.txt" ]
nList = [ 1 , 2 , 3 , 4 ]

nGramDictionary = createNGrams( fileList , nList )

##### Mongo stuff ##############################################

from pymongo import Connection

connection = Connection()

grams = connection.test.grams

grams.drop()

for i in nGramDictionary:
	grams.insert( i )

