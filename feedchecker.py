#!/usr/bin/python
import sys
import zipfile
import re
import collections
import csv
import json
from collections import OrderedDict

csv.field_size_limit(sys.maxsize)

	
def main(filepath, delimiter):

	# Set up variables
	validated_feed = {
		'overall':{},
		'category':{},
		'product':{},
		'product in category': {}
		} # Hash that will be returned to front-end
	invalidlist = []
	feedFile = filepath # grabs filepath from args
	catIds = list()
	prodIds = list()
	parentProdIds = list()
	recommendable = dict()

	delim = "|" # Sets up a default delimiter
	if(delimiter):
		delim=delimiter

	##Open files method. Used to open a file and return the descriptor
	ff = open(feedFile,'r+')

	##Counts if the number of fields per line are consistent with the header
	def fieldCount(f,zipF):
		ff = zipF.open(f)
		perLine = ff.readlines()
		# print "Header Line: "+perLine[0]
		mainCount = perLine[0].count(delim)
		mainCount = mainCount + 1
		ct = 0
		colCountErr = list()
		# print "Number of fields sent are: ",mainCount
		for line in perLine :		
			pipeCount = line.count(delim)
			if ((pipeCount+1) != mainCount):
				#print perLine
				colCountErr.append(ct)
			ct = ct + 1
		return colCountErr


	##File Name Format Check 
	def fileNameCheck(feedFile):
		print '-------------------------------------'
		print "Feed file Name ----> ", feedFile
		pattern = re.search('catalog_full.*.zip',feedFile)
		if(pattern is None):
			validated_feed['overall']['filename_format'] = ['Filename Format Check', False, "The catalog filename format doesn\'t match the format catalog_full_sitename_.*.zip"]
			print '|The catalog filename format doesn\'t match the format catalog_full_sitename_.*.zip|'
		else: 
			validated_feed['overall']['filename_format'] = ['Filename Format Check', True, 'SUCCESS']
			print '|Catalog filename format check ---->|' 

	##Zip file open and process
	def zf(feedFile):
		print '-------------------------------------'
		print 'Zip file open and process'
		zipF = zipfile.ZipFile(feedFile)
		fileArr = zipF.namelist()
		print 'Following are the contents of the catalog feed file:'
		print fileArr
		if fileArr:
			validated_feed['overall']['catalog_feed_contents'] = ['Contents of Catalog Feed', True, fileArr]
		else:
			validated_feed['overall']['catalog_feed_contents'] = ['Contents of Catalog Feed', False, "File Not Found"]
		c = len(fileArr)
		fileArr.sort()
		fileArr1 = [None]*10
		fileStr = delim.join(fileArr)
		feedStatus = flatFileChecker(fileStr,c)
		# if feedStatus is 'pass':
		# 	print 'Next steps are DATA Validation'
		#for f in fileArr:
		#	if re.search('^category_full.*.txt',f): 
		#categoryFullCheck(fileArr[0],zipF,fileArr)
		#	elif re.search('^product_full.*.txt',f):
		#productFullCheck(fileArr[2],zipF,fileArr)
		#	elif re.search('^product_in_category.*.txt',f):
		#prodCatCheck(fileArr[3],zipF,fileArr)
		#	elif re.search('^product_attribute.*.txt',f):
		#prodAttCheck(fileArr[1],zipF,fileArr)
		#	else: 
		#		print 'Unidentified file type ---------> ', f
			#	exit(0)
		for f in fileArr:
			if re.search('^category_full.*.txt',f):
				fileArr1[0] = f
			elif re.search('^product_full.*.txt',f):
				fileArr1[1] = f
			elif re.search('^product_in_category.*.txt',f):
				fileArr1[2] = f
			elif re.search('^product_attribute.*.txt',f):
				fileArr1[3] = f
			elif re.search('^localized_product.*.txt',f):
				fileArr1[4] = f
		if(fileArr1[0]):
			categoryFullCheck(fileArr1[0],zipF,fileArr)
		if(fileArr1[1]):
			productFullCheck(fileArr1[1],zipF,fileArr)
		if(fileArr1[2]):
			prodCatCheck(fileArr1[2],zipF,fileArr)
		if(fileArr1[3]):
			prodAttCheck(fileArr1[3],zipF,fileArr)
		if(fileArr1[4]):
			localProdCheck(fileArr1[4],zipF,fileArr)
		#	else: 
		#		print 'Unidentified file type ---------> ', f

	#Category Full file Checker
	def categoryFullCheck(f,zipF,files):
		print '-----------------------------------------------'
		print 'Category Full file Checker'
		cfD = zipF.open(f) 
		print f + ' File validation in progress'
		catLine = cfD.readlines()
		theDict = fileOpen(f,zipF)
		catCount = fieldCount(f,zipF)
		#catDict = csv.DictReader(cfD2,delimiter='|')
		catHeader = catLine[0].rstrip()
		catHeaderRes = headerCheck(catHeader,f)
		if catCount:
			validated_feed['category']['cat_column_count'] = ['Category Column Count', False, catCount]
			print "Column Count Check Status ----> FAILED ",catCount
		else:
			validated_feed['category']['cat_column_count'] = ['Category Column Count', True, 'SUCCESS']
			print "Column Count Check Status ----> SUCCESS"

		if catHeaderRes is 1: 
			print 'Category File Headers Check Status ----> SUCCESS'
			validated_feed['category']['cat_file_headers_check'] = ['Category File Headers Check', True, 'SUCCESS']
		else:
			print 'Category File Headers Check Status ----> FAILED'
			validated_feed['category']['cat_file_headers_check'] = ['Category File Headers Check', False, 'FAILED']

		parents = list()
		for rows in theDict:	
			if('parent_id' in rows.keys()):
				parents.append(rows['parent_id'])
			catIds.append(rows['category_id'])
		theDict1 = fileOpen(f,zipF)
		numOfCategories = len(catIds)
		chkSetofCat = len(set(catIds))
		catSet = set(catIds)
		catIdRepeat = list()
		#catIdRepeat = set ([x for x in catIds if catIds.count(x)>1])
		for c in catIds:
			if c in catSet:
				catSet.remove(c)
			else:
				catIdRepeat.append(c)
		if len(catIdRepeat) == 0:
			validated_feed['category']['repeat_cat_ids'] = ['Repeating Category IDs', True, 'None']
		else:
			validated_feed['category']['repeat_cat_ids'] = ['Repeating Category IDs', False, catIdRepeat]

		if(numOfCategories!=chkSetofCat):
			print "The following are repeating category ids:"
			for y in catIdRepeat:
				print y
			print '----------->'

		parents = set(parents)
		#print parents
		uniqueParentCount = len(parents)
		listDict = list(theDict1)
		invalidParents = []

		if(rows['parent_id']):
			for item in parents:
				flag = 0
				if item != '':
					for rows in listDict:
						if item == rows['category_id']:
							flag = 1
							break
						
					if flag != 1 :
						print "INVALID PARENT ---->", item
						invalidParents.append(item)
		
		if len(invalidParents) == 0:
			validated_feed['category']['invalid_parent']= ['Invalid Parent IDs', True, "None"]
		else:
			validated_feed['category']['invalid_parent']= ['Invalid Parent IDs', False, invalidParents]

		print "Number of unique categories are:", len(set(catIds))

		validated_feed['category']['unique_cat_count'] = ['Number of Unique Categories', True, len(set(catIds))]

	#Product Full file Checker
	def productFullCheck(f,zipF,files):
		print '-----------------------------------------------'
		print 'Product Full file Checker'
		pfD = zipF.open(f) 
		print f + ' File validation in progress'
		prodLine = pfD.readlines()
		prodDict = fileOpen(f,zipF)
		prodCount = fieldCount(f,zipF)
		#catDict = csv.DictReader(pfD2,delimiter='|')
		prodHeader = prodLine[0].rstrip()
		prodHeaderRes = headerCheck(prodHeader,f)
		if prodHeaderRes is 1: 
			print 'Product Full File Headers Check Status ----> SUCCESS'
			validated_feed['product']['product_full_file_headers'] = ['Product Full File Headers Check', True, 'SUCCESS']
		else:
			print 'Product Full Headers Check Status ----> FAILED'
			validated_feed['product']['product_full_file_headers'] = ['Product Full File Headers Check', False, 'FAILED']
		if prodCount:
			print "Column Count Check Status ----> FAILED ",prodCount
			validated_feed['product']['prod_column_count_check'] = ['Product Column Count Check', False, prodCount]
		else:
			print "Column Count Check Status ----> SUCCESS"
			validated_feed['product']['prod_column_count_check'] = ['Product Column Count Check', True, 'SUCCESS']

		noPriceCount = 0
		noRecCount = 0
		for rows in prodDict:	
			prodIds.append(rows['product_id'])
			if('product_parent_id' in rows):
				if(rows['product_parent_id']):
					parentProdIds.append(rows['product_parent_id'])
			#recommendable[rows['product_id']] = rows['price']
			if rows['price']== '':
				noPriceCount += 1
			if('recommendable' in rows):
				if rows['recommendable'] == 'false' or rows['recommendable'] == '' or rows['recommendable'] == '0':
					noRecCount += 1
		print "Number of Products with no Price ---->", noPriceCount
		if(noPriceCount == 0): 
			validated_feed['product']['products_without_price'] = ['Number of Products Without Price', True, noPriceCount]
		else:
			validated_feed['product']['products_without_price'] = ['Number of Products Without Price', False, noPriceCount]	
		print "Number of Products with recommendable as False ---->", noRecCount
		if(noRecCount == 0):
			validated_feed['product']['products_with_false_recommendable'] = ['Number of Products with Recommendable as False', True, noRecCount]
		else:
			validated_feed['product']['products_with_false_recommendable'] = ['Number of Products with Recommendable as False', False, noRecCount]

		theDict1 = fileOpen(f,zipF)
		numOfProd = len(prodIds)
		chkSetofProd = len(set(prodIds))
		prodSet = set(prodIds)
		parentProdSet  = set(parentProdIds)
		prodIdRepeat = list()
		#prodIdRepeat = set ([x for x in prodIds if prodIds.count(x)>1])
		for p in prodIds:
			if p in prodSet:
				prodSet.remove(p)
			else:
				prodIdRepeat.append(p)

		if(numOfProd!=chkSetofProd):
			print "The following are repeating product ids:"
			print set(prodIdRepeat)
			print '----------->'
			validated_feed['product']['repeat_product_ids'] = ['Repeating Product IDs', False, set(prodIdRepeat)]

		else: 
			print "Product Ids uniqueness Check ----> SUCCESS"
			validated_feed['product']['repeat_product_ids'] = ['Repeating Product IDs', True, 0]
		listDict = list(theDict1)
		for item in parentProdSet:
			flag = 0
			if item != '':
				for row in listDict:
					if item == row['product_id']:
						flag = 1
						break
					
				if flag != 1 :
					invalidlist.append(item)
					print "INVALID PARENT PRODUCT ---->", item

		if len(invalidlist) == 0:
			validated_feed['product']['invalid_parent_product'] = ['Invalid Parent Product IDs', True, "None"]
		else:
			validated_feed['product']['invalid_parent_product'] = ['Invalid Parent Product IDs', False, invalidlist]
			



	#Product in Category file Checker
	def prodCatCheck(f,zipF,files):
		print '-----------------------------------------------'
		print 'Product in Category file Checker'
		pcfD = zipF.open(f) 
		print f + ' File validation in progress'
		pcLine = pcfD.readlines()
		pcDict = fileOpen(f,zipF)
		pcCount = fieldCount(f,zipF)
		pcHeader = pcLine[0].rstrip()
		pcHeaderRes = headerCheck(pcHeader,f)
		if pcHeaderRes is 1: 
			print 'Product in Category File Headers Check Status ----> SUCCESS'
			validated_feed['product in category']['product_in_category_file_headers'] = ['Product in Category File Headers Check', True, 'SUCCESS']
		else:
			print 'Product in Category File Headers Check Status ----> FAILED'
			validated_feed['product in category']['product in category']['product in category']['product_in_category_file_headers'] = ['Product in Category File Headers Check', False, 'FAILED']
		if pcCount:
			print "Column Count Check Status ----> FAILED ",pcCount
			validated_feed['product in category']['product in category']['product_in_cat_column_count_check'] = ['Product in Category File Column Count Check', False, pcCount]
		else:
			print "Column Count Check Status ----> SUCCESS"
			validated_feed['product in category']['product_in_cat_column_count_check'] = ['Product in Category File Column Count Check', True, 'SUCCESS']
		pcProd = list()
		pcCat = list()
		for rows in pcDict:	
			pcProd.append(rows['product_id'])
			pcCat.append(rows['category_id'])
		invalidProd = list()
		invalidCat = list()
		setProd = set(prodIds)
		setCat = set(catIds)
		pcProdSet = set(pcProd)
		pcCatSet = set(pcCat)

		for prod in set(pcProd):
			if prod not in set(prodIds):
				invalidProd.append(prod)
		print "CAT IDS in set --->", set(pcCat)
		#print "MASTER CAT --->", catIds
		for cat in set(pcCat):
			if cat not in set(catIds):
				invalidCat.append(cat)

		# print 'MASTER CAT SET---->',setCat 
		# print 'pc CAT SET',pcCatSet
		invalidProd = list(pcProdSet - setProd)
		invalidCat = list(pcCatSet - setCat)
		if invalidCat:
		 	print "Categories not in the category_full file are: ----> FAILED", invalidCat
		 	validated_feed['product in category']['category_not_in_cat'] = ['Categories Not in category_full File', False, invalidCat]
		else:
		 	print "Categories are all valid ----> SUCCESS"
		 	validated_feed['product in category']['category_not_in_cat'] = ['Categories Not in category_full File', True, 'None']

		if invalidProd:
		 	print "Products not in the product_full file are: ----> FAILED", invalidProd
		 	validated_feed['product in category']['product_not_in_prod'] = ['Products Not in product_full File', False, invalidProd]
		else:
		 	print "Products are all valid ----> SUCCESS"
		 	validated_feed['product in category']['product_not_in_prod'] = ['Products Not in product_full File', True, 'None']



	#Product Attribute File Checker
	def prodAttCheck(f,zipF,files):
		print '-----------------------------------------------'
		print 'Product Attribute File Checker'
		pAttfD = zipF.open(f) 
		print f + ' File validation in progress'
		pAttLine = pAttfD.readlines()
		pAttDict = fileOpen(f,zipF)
		pAttCount = fieldCount(f,zipF)
		pAttHeader = pAttLine[0].rstrip()
		pAttHeaderRes = headerCheck(pAttHeader,f)
		if pAttHeaderRes is 1: 
			print 'Product in Category File Headers Check Status ----> SUCCESS'
			validated_feed['product']['product_in_attribute_file_headers'] = ['Product Attribute File Headers Check', True, 'SUCCESS']
		else:
			print 'Product in Category File Headers Check Status ----> FAILED'
			validated_feed['product']['product_in_attribute_file_headers'] = ['Product Attribute File Headers Check', False, 'FAILED']


		if pAttCount:
			print "Column Count Check Status ----> FAILED ",pAttCount
			validated_feed['product']['prod_in_att_file_column_count_check'] = ['Product Attribute File Column Count Check', False, pAttCount]

		else:
			print "Column Count Check Status ----> SUCCESS"
			validated_feed['product']['prod_in_att_file_column_count_check'] = ['Product Attribute File Column Count Check', True, 'SUCCESS']

		pAttProd = list()
		for rows in pAttDict:
			pAttProd.append(rows['product_id'])
		invalidProdAtt = list()
		setProd = set(prodIds)
		pAttProdSet = set(pAttProd)
		invalidProdAtt = list(pAttProdSet - setProd)
		if invalidProdAtt:
			print "Products not in the product_full file are: ----> FAILED" , invalidProdAtt
			validated_feed['product']['products_not_in_product_full_file'] = ['Product Attributes Not in the product_full File', False, invalidProdAtt]
		else:
			print "Products are all valid ----> SUCCESS"
			validated_feed['product']['products_not_in_product_full_file'] = ['Product Attributes Not in the product_full File', True, 'SUCCESS']

		print "Product Att",len(pAttProd)
		validated_feed['product']['product_attribute'] = ['Product Attribute Total', True, len(pAttProd)]
		print "Product Att Set",len(pAttProdSet)
		validated_feed['product']['product_attribute_set'] = ['Unique Product Attributes', True, len(pAttProdSet)]
		print "Product Full",len(setProd)
		validated_feed['product']['product_full'] = ['Product Full', True, len(setProd)]
		print validated_feed

	#localized_product check
	def localProdCheck(f,zipF,files):
		print '-----------------------------------------------'
		print 'localized_product check'
		loProdfD = zipF.open(f) 
		print f + ' File validation in progress'
		loProdLine = loProdfD.readlines()
		loProdDict = fileOpen(f,zipF)
		loProdCount = fieldCount(f,zipF)
		loProdHeader = loProdLine[0].rstrip()
		loProdHeaderRes = headerCheck(loProdHeader,f)
		if loProdHeaderRes is 1: 
			print 'Localized Product File Headers Check Status ----> SUCCESS'
			validated_feed['product']['local_product_file_headers'] = ['Localized Product File Headers Check', True, "SUCCESS"]
		else:
			print 'Localized Product  File Headers Check Status ----> FAILED'
			validated_feed['product']['local_product_file_headers'] = ['Localized Product File Headers Check', True, "FAILED"]

		if loProdCount:
			print "Column Count Check Status ----> FAILED ",loProdCount
			validated_feed['product']['local_column_count_check'] = ['Localized Product Column Count Check', False, loProdCount]
		else:
			print "Column Count Check Status ----> SUCCESS"
			validated_feed['product']['local_column_count_check'] = ['Localized Product Column Count Check', True, "SUCCESS"]
		loProdProd = list()
		for rows in loProdDict:
			loProdProd.append(rows['product_id'])
		invalidloProd = list()
		setProd = set(prodIds)
		loProdProdSet = set(loProdProd)
		invalidloProd = list(loProdProdSet - setProd)
		if invalidloProd:
			print "Products not in the product_full file are: ----> FAILED" #, invalidloProd
			validated_feed['product']['prod_not_in_prod_full'] = ['Local Products Not in the product_full File', False, invalidloProd]
		else:
			print "Products are all valid ----> SUCCESS"
			validated_feed['product']['prod_not_in_prod_full'] = ['Local Products Not in the product_full File', True, "None"]
		print "Product Att",len(loProdProd)
		validated_feed['product']['local_product_attribute'] = ['Local Product Attribute Total', True, len(loProdProd)]
		print "Product Att Set",len(loProdProdSet)
		validated_feed['product']['local_product_attribute_set'] = ['Unique Local Product Attribute Count', True, len(loProdProdSet)]
		print "Product Full",len(setProd)
		validated_feed['product']['local_product_full'] = ['Local Product Full', True, len(setProd)]



	#Zip Content File Open and Reset
	def fileOpen(f,zipF):
		print '-----------------------------------------------'
		print 'Zip Content File Open and Reset'
		fd = zipF.open(f) 
		theDict = csv.DictReader(fd,delimiter=delim)
		return theDict
		validated_feed['overall']['dictionary'] = ['Dictionary', True, theDict]


	#Header Check Function 
	def headerCheck(header,f):
		print '-----------------------------------------------'
		print 'Header Check Function'
		if re.search('^category_full.*.txt',f):
			checkArray = ['category_id','parent_id','name']	
			validated_feed['overall']['unidentified_file_type'] = ['Unidentified File Type', True, "None"]
		elif re.search('^product_full.*.txt',f):
			checkArray = ['product_id','name','price','recommendable','link_url','image_url']  
	        elif re.search('^product_in_category.*.txt',f):
		        checkArray = ['category_id','product_id']  
	        elif re.search('^product_attribute.*.txt',f):
	                checkArray = ['product_id','attr_name','attr_value']   
	        elif re.search('^localized_product.*.txt',f):
	                checkArray = ['product_id','name','description','language_tag','image_url','link_url']   
	        else:
			print 'Unidentified file type ---------> ', f
			validated_feed['overall']['unidentified_file_type'] = ['Unidentified File Type', False, f]

		flag1 = 1
		for i in checkArray:
			flagArray = []
			if re.search(i,header):
				flag1 = flag1 and flag1  
	        	else:
				flag1 = 0
			if flag1 is 0:
				print 'Following field in header needs review ---->',i
				flagArray.append(i) 
		if len(flagArray) > 0:
			validated_feed['overall']['header_field_needs_review'] = ['Header Fields That Need Review', False, flagArray]
		else:
			validated_feed['overall']['header_field_needs_review'] = ['Header Fields That Need Review', True, "None"]
		return flag1

	##Flat File Name Checker
	def flatFileChecker(fileStr,c):
		print '-----------------------------------------------'
		print 'Flat File Name Checker'
		if c is 4:
			fileFormat = ['product_full','category_full','product_in_category','product_attribute']
		elif c is 3:
			fileFormat = ['product_full','category_full','product_in_category'] 
		else: 
			fileFormat = fileStr
			# print "The catalog file is invalid! Kindly check contents...", fileStr
		countPass = 0 
		for f in fileFormat:
			flag = 0
			patt = f+'.*.txt'
			resPat = re.search(patt,fileStr)
			print 'Checking for file presence and format...',patt 
			if resPat != None:
				flag =  1 
				countPass = countPass + 1
			if flag is 1:
				print 'File Name Check ----> SUCCESS'
				validated_feed['overall']['flat_file_name_check'] = ['Flat File Name Check', True, "SUCCESS"]
			else:
				print 'File Name Check ----> FAILED'
				validated_feed['overall']['flat_file_name_check'] = ['Flat File Name Check', False, "FAILED"]

		if countPass != c:
			print 'File names within catalog feed aren\'t per required format' 
			return 'pass'
		else:
			return 'pass'

	##Function to parse zip and pass on catalog files for checks
	#fieldCount()
	fileNameCheck(feedFile)
	zf(feedFile)
	#uniqueProdIdCheck()
	ff.close()
	# sorted_feed = OrderedDict(sorted(validated_feed.items(), key=lambda t: t[0]))
	return validated_feed


