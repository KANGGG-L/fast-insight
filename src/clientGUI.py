import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox
from client import Client  # Assuming client.py is in the same directory

class ClientGUI:
    def __init__(self, master):
        self.master = master
        master.title("File Analysis Client")

        self.file_path = ""
        self.report_text = ""

        self.label = tk.Label(master, text="Select a file to analyze:")
        self.label.pack()

        self.select_button = tk.Button(master, text="Select File", command=self.select_file)
        self.select_button.pack()

        self.analyze_button = tk.Button(master, text="Analyze Now", command=self.analyze_file)
        self.analyze_button.pack()

        self.save_button = tk.Button(master, text="Save Report", command=self.save_report, state=tk.DISABLED)
        self.save_button.pack()

        self.report_label = tk.Label(master, text="Report:")
        self.report_label.pack()

        self.report_textbox = tk.Text(master, height=10, width=50)
        self.report_textbox.pack()

        self.client = Client()

    def select_file(self):
        self.file_path = filedialog.askopenfilename()
        if self.file_path:
            self.analyze_button.config(state=tk.NORMAL)
            self.save_button.config(state=tk.DISABLED)  # Reset save button state
            self.report_textbox.delete(1.0, tk.END)  # Clear previous report

    def analyze_file(self):
        if not self.file_path:
            messagebox.showerror("Error", "Please select a file to analyze.")
            return

        try:
            report = self.client.analyse_file(self.file_path)
            self.report_text = report.decode()

            self.report_textbox.delete(1.0, tk.END)  # Clear previous report
            self.report_textbox.insert(tk.END, self.report_text)

            self.save_button.config(state=tk.NORMAL)

        except Exception as e:
            messagebox.showerror("Error", f"An error occurred: {str(e)}")

    def save_report(self):
        if not self.report_text:
            messagebox.showerror("Error", "No report to save.")
            return

        save_path = filedialog.asksaveasfilename(defaultextension=".txt")
        if save_path:
            try:
                with open(save_path, "w") as file:
                    file.write(self.report_text)
                messagebox.showinfo("Success", "Report saved successfully.")
            except Exception as e:
                messagebox.showerror("Error", f"An error occurred while saving: {str(e)}")

def main():
    root = tk.Tk()
    app = ClientGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
