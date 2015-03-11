class Tiger:
	def Jump(self):
		print("Jump Jump")

class Lion:
	def Bite(self):
		print("Bite Bite")

class Liger(Tiger, Lion):
	def Play(self):
		print("play play")


l=Liger()

l.Bite()

l.Jump()

l.Play()
