# Creates chunck from twitter data
# The file is huge : create subfiles of n_lines

import re

n_lines = 100000
line_read = 1

append = False

writeTo = '/Users/lcambier/TempMapReduce/texts'
fwrite = open(writeTo,'w')

with open('/Users/lcambier/TempMapReduce/2014_04_28_tweets_from_stream_3.csv') as f:
    for line in f:
		linen = line.rstrip('\n') 		
		
		if line_read % 10000 == 0:
			print('Line {}'.format(line_read))

		if append:			
			idxComma = linen.find('",')
			# Si on trouve pas, on append = True et on passe a la ligne suivante en fusionant les stirng
			if idxComma == -1:
				remaining = remaining + linen	
			else:				
				text = remaining + linen[:idxComma]				
				fwrite.write(text + '\n') 
				append = False
		else:	
			# On cherche la premiere virgule
			idxComma = linen.find(',')
			remaining = linen[idxComma+1:]
			if len(remaining) != 0:
				# Cas 1) Si 2e element commence pas par " : choper tout jusqu'a la virgule	
				if remaining[0] != '"':
					idxComma = remaining.find(',')
					text = remaining[:idxComma]	
					fwrite.write(text + '\n') 			
				# Cas 2) On a ,"  Dans ce cas, on attend jusqu'a retrouver un ", qui ne soit pas "",
				else:				
					remaining = remaining[1:]
					# On cherche ",				
					idxComma = remaining.find('",')				
					# Si on trouve pas, on append = True et on passe a la ligne suivante
					if idxComma == -1:
						append = True		
					# Si on trouve, le string ca de ," a ",
					else:
						text = remaining[:idxComma]
						fwrite.write(text + '\n') 					

		line_read = line_read+1

fwrite.close()