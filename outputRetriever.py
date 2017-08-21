

import os, sys, string 
import subprocess 

try:
	import boto3 
except:
	pass 

sys.path.append(os.getcwd())
from utilPrinting import * 



class outputRetriever:
	supported_tools = ["scp", "rsync", "s3", "test"]
	def __init__(self, tool):
		""" a class supports using different tools for retrieving output, 
			currently supports scp, rsync, S3 
		
		Arguments: 
			tool {str} -- different tools for retrieving output, 
		"""

		self.tool = tool.lower()
		if tool.lower() not in outputRetriever.supported_tools: 
			WARNING("{} not supported, only {} are supported, using ssh instead".\
				format(tool, outputRetriever.supported_tools))
		
		self.f = getattr(self, "retrieve_output_{}".format(self.tool)) 
		assert callable(self.f), "outputRetriever function {} not callable".format(f) 



	def retrieve(self, username, node, source, destination="./", supplemental_err_msg=None):
		""" high level method exposed to user, it binds to the specific retriever during initialization
		
		[description]
		
		Arguments:
			username {[type]} -- [description]
			node {[type]} -- [description]
			source {[type]} -- [description]
		
		Keyword Arguments:
			destination {str} -- [description] (default: {"./"})
			supplemental_err_msg {[type]} -- [description] (default: {None})
		"""
		self.f(username, node, source, destination, supplemental_err_msg)


	def retrieve_output_scp(self, username, node, source, destination, supplemental_err_msg=None):
		# retrieve output, use rsync may give much better performance if output is huge 
		p = subprocess.run("scp -r {}@{}:{} {}".format(username, node, source, destination), 
						shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
		if len(p.stderr):
			WARNING("failed to retrieve from node {}:{} to {}, error {}, more msg: {}".format(
				node, source, destination, p.stderr.decode("utf-8").strip(string.whitespace), supplemental_err_msg))
		return -1 


	def retrieve_output_rsync(self, username, node, source, destination, supplemental_err_msg=None):
		# retrieve output, use rsync may give much better performance if output is huge 
		print("rsync not supported yet") 
		return -1 


	def retrieve_output_s3(self, username, node, source, destination, supplemental_err_msg=None):
		

		
		# retrieve output, use rsync may give much better performance if output is huge 
		p = subprocess.run("scp -r {}@{}:{} {}".format(self.username, node, source, destination), 
						shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
		if len(p.stderr):
			WARNING("failed to retrieve from node {}:{} to {}, error {}, more msg: {}".format(
				node, source, destination, p.stderr.decode("utf-8").strip(string.whitespace), supplemental_err_msg))
		return -1 


	def retrieve_output_test(self, username, node, source, destination, supplemental_err_msg=None):
		print("this is retrieve test")


if __name__ == "__main__":
	outputRetriever("test").retrieve(None, None, None, None, None)