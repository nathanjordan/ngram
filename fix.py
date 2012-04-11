import pymongo
import sys
import re

from pymongo import Connection
from pymongo.code import Code

connection = Connection()
grams = connection.test.grams
grams2 = connection.test.grams2
grams3 = connection.test.grams3
grams4 = connection.test.grams4
grams5 = connection.test.grams5
grams6 = connection.test.grams6

map = Code("function map() { emit( this.word , { count : 1 } ); }")
reduce = Code("function reduce( key , values ) { var result = { count : 0 }; values.forEach( function( value ) { result.count += value.count; }); return result; }")

#map reduce that shizzz
grams4.map_reduce( map , reduce , "grams5" )

j = 0
for i in grams5.find():
	grams6.insert( { "word" : i["_id"] , "count" : i["value"]["count"] , "lowerBound" : j , "upperBound" : j + i["value"]["count"] } )
	j += i["value"]["count"]
