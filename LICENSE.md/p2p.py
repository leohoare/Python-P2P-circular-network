import sys
import time
import socket
import threading
import struct

# definitions of variables
PORT_BASE = 50000 #each peer listens to 50000 + i
LOCALIP = "127.0.0.1"
PINGTIMEOUT = 1.0
PINGFREQ = 4.0 #how often ping messages are sent
UDPBUFFER = 100 # maximum bytes ping buffer can be
TCPBUFFER = 1024 # maximum byes TCP channel can be
ACK_ACCUMULATION_MAX = 5 #max amount of ACKS before assumes peer is dead


#dictionary to determine message types
# messages can be called either way to help make code readable.
MESSAGETYPE = {0:'pingreq', 1:'pingres', 2:'filereq', 3:'fileres', 4:'lostpeer', 5:'quit'}
ENCR_MTYPE = {'pingreq':0, 'pingres':1, 'filereq':2, 'fileres':3, 'lostpeer':4, 'quit':5}


class Peer:
	def __init__(self,id,pred,succ1,succ2):
		self.id = id
		self.port = PORT_BASE + id
		self.pred = None
		self.succ1 = succ1
		self.succ2 = succ2


		# 5 threads

		#function listening for input
		t0 = threading.Thread(target=self.input_function)
		t0.start()

		#function pinging successor 1
		t1 = threading.Thread(target = self.send_ping, args = (1,))
		t1.daemon = True
		t1.start()

		#function pinging sucessor 2
		t2 = threading.Thread(target = self.send_ping, args = (2,))
		t2.daemon = True
		t2.start()

		#function listening to UDP
		t3 = threading.Thread(target=self.listen_UDP)
		t3.daemon = True
		t3.start()

		#function listening to TCP
		t4 = threading.Thread(target=self.listen_TCP)
		t4.daemon = True
		t4.start()

	

	# creates byte array message to incorporate sender ID and extra information when needed
	def message_helper(self, mtype, info1 = None, info2 = None, info3 = None):
		message = bytearray([mtype])
		message.extend( struct.pack("B", self.id))
		if info1 != None:
			message.extend( struct.pack("B", info1))
		if info2 != None:
			message.extend( struct.pack("B", info2))
		if info3 != None:
			message.extend( struct.pack("B", info3))	
		return message

	# finds next sequence number
	def next_sequence(self,past_sequence):
		return (past_sequence+1)%256

	# evaluates the amount of sequences that have been missed
	def sequence_evaluator(self,last_sequence,next_sequence):
		if last_sequence > next_sequence:
			return 255 - last_sequence + next_sequence + 1 #1 because zero case
		else:
			return next_sequence - last_sequence

	# If no ping ACK and a peer is dead, this function updates peers through TCP
	# divided into the two cases for succ1 and succ2
	# succ1 updates immediately moving second forward then pinging that successor
	# succ2 waits for ping freq for the next sucessor to update then contacts for information
	def lost_peer(self, succ_number):
		TCP_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		message = self.message_helper(ENCR_MTYPE['lostpeer'])
		if succ_number == 1: #assume first sucessor is lost
			self.succ1 = self.succ2
			TCP_socket.connect((LOCALIP,PORT_BASE+self.succ2))
		elif succ_number == 2:
			time.sleep(PINGFREQ+1) # sleep long enough to allow sucessors sucessor to update
			TCP_socket.connect((LOCALIP,PORT_BASE+self.succ1))
		TCP_socket.send(message)
		data = TCP_socket.recvfrom(TCPBUFFER)
		self.succ2 = int(data[0])
		TCP_socket.close()
		if self.succ1 == self.id: #case for two nodes and one quits
			print("I'm last peer on network, I have no successors")
			self.succ1 = None
			self.succ2 = None
		else:
			print(f'My first successor is now peer {self.succ1}')
			print(f'My second sucessor is now peer {self.succ2}')


	# function to send_pings via UDP to successors
	def send_ping(self,indicator):
		last_sequence = 0
		sequenceNum = 0
		while True:
			if indicator == 1:
				targetID = self.succ1
			else:
				targetID = self.succ2
			if not targetID or targetID == self.id:
				return
			UDP_socket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
			message = self.message_helper(ENCR_MTYPE['pingreq'],indicator,sequenceNum)
			# sets timeout if package isn't received in time (1 second in this case)
			UDP_socket.settimeout(PINGTIMEOUT)
			UDP_socket.sendto(message,(LOCALIP,PORT_BASE+targetID))
			try:
				data,addr = UDP_socket.recvfrom(UDPBUFFER)
				responseID = int(data[1])
				if MESSAGETYPE[data[0]] == 'pingres':
					print(f"A ping response message was received from Peer {responseID}")
					last_sequence = sequenceNum
			except socket.timeout:
				ACK_accumulation = self.sequence_evaluator(last_sequence,sequenceNum)
				if ACK_accumulation:
					print(f"No ping response from Peer {targetID}, no ACK's accumulation = {ACK_accumulation}, sequence gap = [ {last_sequence} - {sequenceNum} ]")
				if ACK_accumulation > ACK_ACCUMULATION_MAX: #succ is dead
					print(f'Peer {targetID} is no longer alive.')
					self.lost_peer(indicator)
			sequenceNum = self.next_sequence(sequenceNum)
			time.sleep(PINGFREQ)
	

	#function to listen to UDP message and return messages
	def listen_UDP(self):
		Server_socket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
		Server_socket.bind((LOCALIP,self.port))
		while 1:
			data, addr = Server_socket.recvfrom(UDPBUFFER)
			if not data: break
			senderID = int(data[1])
			if MESSAGETYPE[data[0]] == 'pingreq':
				## if Ping is from first sucessor, update the predecessor qui
				if data[2] == 1:
					self.pred = int(senderID)
				print(f"A ping request message was received from Peer {senderID}")
				message = self.message_helper(ENCR_MTYPE['pingres'],data[3])
				Server_socket.sendto(message,addr)


	#function for listening to TCP messages
	def listen_TCP(self):
		Server_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
		Server_socket.bind((LOCALIP,self.port))
		Server_socket.listen()
		while 1:
			Connection_socket, addr = Server_socket.accept()
			data = Connection_socket.recv(TCPBUFFER)
	
			try:
				if MESSAGETYPE[data[0]] == 'filereq' : #file request from peer
					TCP_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
					# evaluates if it is held at my location
					# base case, general case, smaller than smallest peer and case of larger than highest peer,
					if self.id == data[2] or \
					(self.pred < data[2] <= self.id) or \
					(self.succ1 < self.id and data[2] > self.id) or\
					(self.pred > self.id and data[2] < self.id) :
						print(f'File {data[3]*256 + data[2]} is here.')
						print(f'A response message, destined for Peer {data[1]}, has been sent')
						TCP_socket.connect((LOCALIP,PORT_BASE+data[1]))
						message = self.message_helper(ENCR_MTYPE['fileres'],data[2],data[3])
						TCP_socket.send(message) 
					else: #forward the message to the next peer
						print(f'File {data[3]*256 + data[2]} is not stored here.')
						print('File request has been forwarded to my sucessor')
						TCP_socket.connect((LOCALIP,PORT_BASE+self.succ1))
						TCP_socket.send(data)
					TCP_socket.close()

				elif MESSAGETYPE[data[0]] == 'fileres': #file response from peer
					print(f'Received a response message from peer {data[1]}, which has file {data[3]*256 + data[2]}')

				elif MESSAGETYPE[data[0]] == 'quit': # quit message received (update sucessors)
					if int(data[1]) == self.succ1:
						TCP_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
						if int(data[2]) == self.id:
							print("I'm last peer on network, I have no successors")
							self.succ1 = None
							self.succ2 = None
						else: 
							self.succ1 = int(data[2])
							self.succ2 = int(data[3])
						TCP_socket.connect((LOCALIP,PORT_BASE+self.pred))
						TCP_socket.send(data)
						TCP_socket.close()
					elif int(data[1]) == self.succ2:
						self.succ2 = int(data[2])
					if self.succ1 and self.succ2:
						print(f'My first successor is now peer {self.succ1}')
						print(f'My second sucessor is now peer {self.succ2}')


				elif MESSAGETYPE[data[0]] == 'lostpeer': #lost peer message received
					# return a message containing my successor
					message = str(self.succ1).encode('utf-8')
					Connection_socket.send(message)

				data = None
				
			except ConnectionRefusedError:
				print("Couldn't connect to peer, please wait until peers update and try again")
			
			Connection_socket.close()


	# if file request is called in input
	def file_function(self,filenum):
		filehash = filenum % 256
		filevalue = filenum // 256
		if not self.pred: # if predecessors are None, they need t update
			print("please wait until predecessors update")
		#checks if the file is located at my location
		elif filehash == self.id or \
		self.succ1 == None or \
		(self.pred < filehash <= self.id) or \
		(self.succ1 < self.id and filehash > self.id) or \
		(self.pred > self.id and filehash < self.id):
			print(f"File {filenum} is found here in peer {self.id}")
		
		else: #else it is forwarded to next peer
			try:
				print(f'File {filenum} is not stored here.')
				print('File request has been forwarded to my sucessor')

				message = self.message_helper(ENCR_MTYPE['filereq'], filehash, filevalue)
				TCP_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				TCP_socket.connect((LOCALIP,PORT_BASE+self.succ1))
				TCP_socket.send(message)
				TCP_socket.close()
			except ConnectionRefusedError:
				print("Couldn't connect to peer, please wait until peers update and try again")

	#if quit is called, it informs the peers of departure
	def quit_function(self):
		if self.pred == None:
			time.sleep(PINGFREQ+1) #waits 5 seconds to check if predecessors just havent been updated.
		try:
			print(f'Peer {self.id} will depart from the network')
			print("please allow a few seconds before subsuquent quit functions to allow peers to update")
			message = self.message_helper(ENCR_MTYPE['quit'],self.succ1,self.succ2)
			TCP_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			TCP_socket.connect((LOCALIP,PORT_BASE+self.pred))
			TCP_socket.send(message)
			TCP_socket.close()
			sys.exit(0)
		except (TypeError,ConnectionRefusedError): #case where it's the only node left
			sys.exit(0)
		
	# function to constantly listen to input
	def input_function(self):
		while(1):
			inp = input("")
			inp.strip()
			if inp == 'quit':
				self.quit_function()
			elif inp[0:7] == 'request':
				try:
					filenum = int(inp[8:])
					if not 0 <= filenum <= 9999:
						raise ValueError
					self.file_function(filenum)
				except ValueError:
					print("incorrect input: file number incorrect")
			else:
				print("Incorrect input: input doesn't match any function")


	

if __name__ == "__main__":

	## ensures correct amount of arguments entered (3) and  ##
	## ensure of the right format (integer) and ensure right range 0 <= arg <= 255 ##
	try:
		if len(sys.argv) != 4:
			raise ValueError
		for i in range(1,len(sys.argv)):
			sys.argv[i]=int(sys.argv[i])
			if not 0 <= sys.argv[i] <= 255:
				raise ValueError
	except ValueError:
		for i in range(len(sys.argv)):
			print(sys.argv[i])
		print("Incorrect arguments entered")
		sys.exit()

	#sets up peer with class functionality
	peer = Peer(sys.argv[1],None,sys.argv[2],sys.argv[3])