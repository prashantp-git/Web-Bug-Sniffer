from flask import Flask,render_template
from bson import ObjectId
from pymongo import MongoClient
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import os
import ast
import base64
import io
import operator
import numpy as np

app = Flask(__name__)

def getData(keyType):
	client=MongoClient("localhost:27017")
	db=client.logdata2
	logc=db.logcollection
	try:
		emp=logc.find()
		#print emp
		for entry in emp:
			stent=str(entry)
			entry=stent.replace("u'","'")
			entry=entry.replace("':","' :")
			entry=entry.replace("ObjectId(","")
			entry=entry.replace(")","")
			dicen=ast.literal_eval(entry)
			if dicen['keyType']==keyType:
				if keyType=='ResCodesAll' or keyType=='ResCodes4xx' or keyType=='ResCodes5xx':
					temp=dicen['keyVal']
					temp=temp.replace("=",":")
					finaldic=ast.literal_eval(temp)
					return finaldic
				if keyType=="IpAddress" or keyType=="Resources" or keyType=="ResourcesErrors":
					tempval=dicen['keyVal']
					tempval=tempval[1:len(tempval)-1]
					templist=tempval.split(", ")
					finaldic={}
					for eachent in templist:
						entry=eachent.split("=")
						finaldic[entry[0]]=int(entry[1])
					length=min(10,len(finaldic))
					topfinaldic=dict(sorted(finaldic.iteritems(),key=operator.itemgetter(1),reverse=True)[:length])
					
					return topfinaldic
			
		return {'Nothing':'to display'} 


	except Exception, e:
		print str(e)


@app.route("/ResCodesAll")
def rescodesAll():
	return render_template("table.html",value=getData("ResCodesAll"),col1="Response Code",col2="Count")



@app.route("/ResCodes4xx")
def rescodes4xx():
	return render_template("table.html",value=getData("ResCodes4xx"),col1="Response Code",col2="Count")



@app.route("/ResCodes5xx")
def rescodes5xx():
	return render_template("table.html",value=getData("ResCodes5xx"),col1="Response Code",col2="Count")



@app.route("/Resources")
def resources():
	return render_template("table.html",value=getData("Resources"),col1="Resources",col2="Count")

@app.route("/IpAddress")
def ipaddr():
	return render_template("table.html",value=getData("IpAddress"),col1="Ip/Client",col2="Count")

@app.route("/ResourcesErrors")
def reserror():
	return render_template("table.html",value=getData("ResourcesErrors"),col1="Resources With Errors",col2="Count")


def build_pie(lab, val):
	img = io.BytesIO()
	plt.subplots()
	fig1, ax1 = plt.subplots()
	ax1.pie(val, labels=lab, autopct='%1.1f%%',shadow=True, startangle=90)
	ax1.axis('equal')  
	plt.savefig(img, format='png')
	img.seek(0)
	graph_url = base64.b64encode(img.getvalue()).decode()
	plt.close()
	return 'data:image/png;base64,{}'.format(graph_url)

def build_bar(lab, val):
	img = io.BytesIO()
	plt.subplots()
	#fig1, ax1 = plt.subplots()
	y_co=np.arange(len(lab))
	
	plt.bar(y_co,val)
	plt.xticks(y_co,lab)
	#ax1.axis('equal')  
	plt.savefig(img, format='png')
	img.seek(0)
	graph_url = base64.b64encode(img.getvalue()).decode()
	plt.close()
	return 'data:image/png;base64,{}'.format(graph_url)

@app.route('/ResCodesAllPie')
def resCodeAllPie():
	resDict=getData("ResCodesAll")
	labels=[]
	values=[]
	
	for key in resDict:
		labels.append(key)
		values.append(int(resDict[key])) 
	pie_url = build_pie(labels,values);
	return render_template('image.html',name="ResCodesAll",image=pie_url)

@app.route('/ResCodesAllBar')
def resCodeAllBar():
	resDict=getData("ResCodesAll")
	labels=[]
	values=[]
	
	for key in resDict:
		labels.append(key)
		values.append(int(resDict[key])) 
	pie_url = build_bar(labels,values);
	return render_template('image.html',name="ResCodesAll",image=pie_url)

@app.route('/ResCodes4xxPie')
def resCode4xxPie():
	resDict=getData("ResCodes4xx")
	labels=[]
	values=[]
	
	for key in resDict:
		labels.append(key)
		values.append(int(resDict[key])) 
	pie_url = build_pie(labels,values);
	return render_template('image.html',name="ResCodes4xx",image=pie_url)


@app.route('/ResCodes5xxPie')
def resCode5xxPie():
	resDict=getData("ResCodes5xx")
	labels=[]
	values=[]
	
	for key in resDict:
		labels.append(key)
		values.append(int(resDict[key])) 
	pie_url = build_pie(labels,values);
	return render_template('image.html',name="ResCodes5xx",image=pie_url)


if __name__ == '__main__':
	app.run(debug=True)



