from mongoengine import *
import urllib2
import time
import xml.etree.ElementTree as ET
import pprint
import os


#class code	
class Alias(EmbeddedDocument):
	category = StringField()
	first_name = StringField()
	name= StringField()
	meta = {
        'collection': 'testing1'
    }
class Address(EmbeddedDocument):
	uid = StringField(default=None)
	address1 = StringField(default=None)
	address2 = StringField(default=None)
	postal_code = StringField(default=None)
	city = StringField(default=None)
	country = StringField(default=None)
	meta = {
        'collection': 'testing1'
    }	
class SanctionedEntity(Document):
	unique_id= StringField()
	entity_type= StringField()
	first_name= StringField()
	name= StringField()
	source= StringField()
	has_alias= BooleanField()
	##go back and decide if this method is best
	last_updated= StringField()
	aliases = ListField(EmbeddedDocumentField(Alias), default=None)
	is_current = BooleanField()
	source_file = StringField()
	source_date = StringField()
	address = ListField(EmbeddedDocumentField(Address), default=None)
	has_address = BooleanField(default= False)
	meta = {
        'collection': 'testing1'
    }
	
#variables

added = []
updated = []
dropped = []
all_uid = []

#Opens current SDN list and creates a parsed file
page = urllib2.urlopen("https://www.treasury.gov/ofac/downloads/sdn.xml")
text = page.read()
path = "XML Pulls"
file = "SDN List Full XML Pull " + time.strftime("%d%B%y") + ".txt"
parsed_file = ET.fromstring(text)
publishInformation = parsed_file.findall('{http://tempuri.org/sdnList.xsd}publshInformation')
fullTree = parsed_file.findall('{http://tempuri.org/sdnList.xsd}sdnEntry')



#pulls the date from the field of the xml
for x in publishInformation:
	date = x.find('{http://tempuri.org/sdnList.xsd}Publish_Date').text

#writes new file to "XML Pulls" directory
if date in SanctionedEntity.objects.distinct("source_date"):
	print "List for %s is already saved" % (date)
else:
	with open(os.path.join(path, file), 'w+') as f:
		f.write(str(text))
		f.close
	print "Finished and saved " + file



#adds a new entity to the database. Saves the following information: source, entity type, first name (if applicable), last name, source date, addresses, aliases, collection metadata
def addNewEntity(entity):
	entry = SanctionedEntity(unique_id=entity.find('{http://tempuri.org/sdnList.xsd}uid').text)
	entry.source_file=file
	entry.entity_type= entity.find('{http://tempuri.org/sdnList.xsd}sdnType').text
	if entity.find('{http://tempuri.org/sdnList.xsd}firstName') is not None:
		entry.first_name= entity.find('{http://tempuri.org/sdnList.xsd}firstName').text
	entry.name= entity.find('{http://tempuri.org/sdnList.xsd}lastName').text
	entry.source="Treasury SDN List"
	entry.last_updated=time.strftime("%d%B%y at %H:%M")
	entry.source_date=date
	entry.is_current=True
	if entity.find('{http://tempuri.org/sdnList.xsd}akaList') is not None:
		entry.has_alias=True
		newAliases = []
		for aka in entity.find('{http://tempuri.org/sdnList.xsd}akaList'):
			AKAName = aka.find('{http://tempuri.org/sdnList.xsd}lastName').text
			AKACategory = aka.find('{http://tempuri.org/sdnList.xsd}category').text
			if aka.find('{http://tempuri.org/sdnList.xsd}firstName') is not None:
				AKAFirstName = aka.find('{http://tempuri.org/sdnList.xsd}firstName').text
 				new_Alias = Alias(category=AKACategory, name=AKAName, first_name=AKAFirstName)
			else:
				new_Alias = Alias(category=AKACategory, name=AKAName)
			newAliases.append(new_Alias)
			
		entry.aliases=newAliases
	if entity.find('{http://tempuri.org/sdnList.xsd}addressList') is not None:
		addresses = []
		entry.has_address = True
		for address in entity.find('{http://tempuri.org/sdnList.xsd}addressList'):
			newAddress = Address(uid=address.find('{http://tempuri.org/sdnList.xsd}uid').text)
			if address.find('{http://tempuri.org/sdnList.xsd}address1') is not None:
				newAddress.address1 = address.find('{http://tempuri.org/sdnList.xsd}address1').text
			if address.find('{http://tempuri.org/sdnList.xsd}address2') is not None:
				newAddress.address2 = address.find('{http://tempuri.org/sdnList.xsd}address2').text
			if address.find('{http://tempuri.org/sdnList.xsd}city') is not None:
				newAddress.city = address.find('{http://tempuri.org/sdnList.xsd}city').text
			if address.find('{http://tempuri.org/sdnList.xsd}postalCode') is not None:
				newAddress.postal_code = address.find('{http://tempuri.org/sdnList.xsd}postalCode').text
			if address.find('{http://tempuri.org/sdnList.xsd}country') is not None:
				newAddress.country = address.find('{http://tempuri.org/sdnList.xsd}country').text
			
			addresses.append(newAddress)	
		entry.address = addresses
	entry.save()

# updates the entity's information (source, last updated, date, current, and aliases)

def updateEntity(entity):
	source_file=file
	last_updated=time.strftime("%d%B%y at %H:%M")
	source_date=date
	SanctionedEntity.objects(unique_id=entity.find('{http://tempuri.org/sdnList.xsd}uid').text).update_one(set__source_file=source_file,
	set__source_date=source_date, set__is_current=True, set__last_updated=last_updated)
	if entity.find('{http://tempuri.org/sdnList.xsd}akaList') is not None:
		SanctionedEntity.objects(unique_id=entity.find('{http://tempuri.org/sdnList.xsd}uid').text).update_one(set__has_alias=True)
		for aka in entity.find('{http://tempuri.org/sdnList.xsd}akaList'):
			AKAName = aka.find('{http://tempuri.org/sdnList.xsd}lastName').text
			AKACategory = aka.find('{http://tempuri.org/sdnList.xsd}category').text
			if aka.find('{http://tempuri.org/sdnList.xsd}firstName') is not None:
				AKAFirstName = aka.find('{http://tempuri.org/sdnList.xsd}firstName').text
				new_Alias = Alias(category=AKACategory, name=AKAName, first_name=AKAFirstName)
				SanctionedEntity.objects(unique_id=entity.find('{http://tempuri.org/sdnList.xsd}uid').text).update_one(add_to_set__aliases=new_Alias)
			else:
				new_Alias = Alias(category=AKACategory, name=AKAName)
				SanctionedEntity.objects(unique_id=entity.find('{http://tempuri.org/sdnList.xsd}uid').text).update_one(add_to_set__aliases=new_Alias)
	if entity.find('{http://tempuri.org/sdnList.xsd}addressList') is not None:
		SanctionedEntity.objects(unique_id=entity.find('{http://tempuri.org/sdnList.xsd}uid').text).update_one(set__has_address=True)
		for address in entity.find('{http://tempuri.org/sdnList.xsd}addressList'):
			newAddress = Address(uid=address.find('{http://tempuri.org/sdnList.xsd}uid').text)
			if address.find('{http://tempuri.org/sdnList.xsd}address1') is not None:
				newAddress.address1 = address.find('{http://tempuri.org/sdnList.xsd}address1').text
			if address.find('{http://tempuri.org/sdnList.xsd}address2') is not None:
				newAddress.address2 = address.find('{http://tempuri.org/sdnList.xsd}address2').text
			if address.find('{http://tempuri.org/sdnList.xsd}city') is not None:
				newAddress.city = address.find('{http://tempuri.org/sdnList.xsd}city').text
			if address.find('{http://tempuri.org/sdnList.xsd}postalCode') is not None:
				newAddress.postal_code = address.find('{http://tempuri.org/sdnList.xsd}postalCode').text
			if address.find('{http://tempuri.org/sdnList.xsd}country') is not None:
				newAddress.country = address.find('{http://tempuri.org/sdnList.xsd}country').text
			SanctionedEntity.objects(unique_id=entity.find('{http://tempuri.org/sdnList.xsd}uid').text).update_one(add_to_set__address=newAddress)
			


#creates a list of existing uIDs in DB	
allCurrentIDs = SanctionedEntity.objects(is_current=True).distinct("unique_id")


#checks if record in current SDN list is new or existing. If new, it gets added. If existing, the record is updated
def checkIfExisting(entity, allCurrentIDs):
	if entity.find('{http://tempuri.org/sdnList.xsd}uid').text in allCurrentIDs:
		updateEntity(entity)
		
		print "  Updated %s" % (entity.find('{http://tempuri.org/sdnList.xsd}uid').text)
		updated.append(entity.find('{http://tempuri.org/sdnList.xsd}uid').text)
	else:
		addNewEntity(entity)
		
		print "    Added %s" % (entity.find('{http://tempuri.org/sdnList.xsd}uid').text)
		added.append(entity.find('{http://tempuri.org/sdnList.xsd}uid').text)

#runningcode
# print "Starting to check entities"
for entity in fullTree:
	print "checking %s ...." % (entity.find('{http://tempuri.org/sdnList.xsd}uid').text)
	checkIfExisting(entity, allCurrentIDs)
	all_uid.append(entity.find('{http://tempuri.org/sdnList.xsd}uid').text)
print "Evaluated " + str(len(all_uid))

# check if record is no longer current
for id in allCurrentIDs:
	if id not in all_uid:
		SanctionedEntity.objects(unique_id=id).update_one(set__is_current=False)
		dropped.append(id)
		print "Dropped %s from the list" % (id)


deltaFileName = "delta file " + time.strftime("%d%B%y") + ".txt"	
with open(os.path.join(path, deltaFileName), 'w+') as f:
	f.write("Added: %s, \nUpdated: %s, \nDropped: %s" % (', '.join(added), ', '.join(updated), ', '.join(dropped)))
	f.close


