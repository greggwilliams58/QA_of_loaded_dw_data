import tkinter as tk
from  tkinter import *
import tkinter.messagebox




class MainApplication(tk.Frame):
    def __init__(self, parent, *args, **kwargs):
        tk.Frame.__init__(self, parent, *args, **kwargs)
        self.parent = parent
        
        #<create the rest of your GUI here>
        self.parent.geometry("512x524")
        self.parent.title("The Data Validation App")
        
        self.button1 = Button(self,text="hello",fg="red").pack()
        self.button2 = Button(self,text="goodbye",fg="green").pack(side="right")


if __name__ == "__main__":
    root = tk.Tk()
    MainApplication(root).pack(side="top", fill="both", expand=True)
    

    root.mainloop()