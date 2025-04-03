import tkinter as tk
from tkinter import filedialog, messagebox
from mde_processor.reference_data import ReferenceDataManager

class MDEUploaderGUI:
    """GUI for uploading and processing MDE files"""
    
    def __init__(self, callback):
        self.window = tk.Tk()
        self.window.title("MDE File Uploader")
        self.window.geometry("400x200")
        self.callback = callback  # Callback to pass reference data back
        self.setup_gui()

    def setup_gui(self):
        """Set up the GUI components"""
        tk.Label(self.window, text="Please upload the .mde file containing reference data").pack(pady=20)
        tk.Button(self.window, text="Browse", command=self.upload_file).pack(pady=10)

    def upload_file(self):
        """Handle file upload"""
        file_path = filedialog.askopenfilename(filetypes=[("MDE files", "*.mde"), ("All files", "*.*")])
        if file_path:
            try:
                ref_manager = ReferenceDataManager(file_path)
                ref_manager.load_all_tables()
                self.callback(ref_manager)
                messagebox.showinfo("Success", "MDE file processed successfully")
                self.window.destroy()
            except Exception as e:
                messagebox.showerror("Error", f"Failed to process MDE file: {str(e)}")

    def run(self):
        """Run the GUI"""
        self.window.mainloop()