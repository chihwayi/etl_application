import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
import logging
from core.connection import DatabaseConnection
from config.db_config import DatabaseConfig
from mde_processor.mde_uploader import MDEUploaderGUI  # Import for transition

logger = logging.getLogger(__name__)

class DataValidatorGUI:
    """GUI for validating and updating missing patient data"""
    
    def __init__(self, source_conn):
        self.source_conn = source_conn
        self.window = tk.Tk()
        self.window.title("Patient Data Validator")
        self.window.geometry("800x600")
        self.entries = {}
        self.setup_gui()
        self.load_missing_data()

    def setup_gui(self):
        """Set up the GUI components"""
        self.tree = ttk.Treeview(self.window, columns=("PatientID", "FileRef", "FirstName", "SurName", "Sex"), show="headings")
        self.tree.heading("PatientID", text="Patient ID")
        self.tree.heading("FileRef", text="Cohort Number")
        self.tree.heading("FirstName", text="First Name")
        self.tree.heading("SurName", text="Surname")
        self.tree.heading("Sex", text="Sex")
        self.tree.pack(fill=tk.BOTH, expand=True)

        self.tree.column("PatientID", width=100, anchor="center")
        self.tree.column("FileRef", width=100, anchor="center")
        self.tree.column("FirstName", width=150)
        self.tree.column("SurName", width=150)
        self.tree.column("Sex", width=100)

        input_frame = ttk.Frame(self.window)
        input_frame.pack(pady=10)

        ttk.Label(input_frame, text="First Name:").grid(row=0, column=0, padx=5)
        self.first_name_entry = ttk.Entry(input_frame)
        self.first_name_entry.grid(row=0, column=1, padx=5)

        ttk.Label(input_frame, text="Surname:").grid(row=1, column=0, padx=5)
        self.surname_entry = ttk.Entry(input_frame)
        self.surname_entry.grid(row=1, column=1, padx=5)

        ttk.Label(input_frame, text="Sex:").grid(row=2, column=0, padx=5)
        self.sex_entry = ttk.Entry(input_frame)
        self.sex_entry.grid(row=2, column=1, padx=5)

        ttk.Button(self.window, text="Save Updates", command=self.save_updates).pack(pady=10)

        self.tree.bind("<<TreeviewSelect>>", self.on_select)

    def load_missing_data(self):
        """Load patients with missing required fields"""
        query = """
        SELECT PatientID, FileRef, FirstName, SurName, Sex
        FROM tblpatients
        WHERE FirstName IS NULL OR FirstName = ''
        OR SurName IS NULL OR SurName = ''
        OR Sex IS NULL OR Sex = ''
        """
        try:
            df = self.source_conn.execute_query(query)
            for _, row in df.iterrows():
                self.tree.insert("", "end", values=(
                    row["PatientID"],
                    row["FileRef"],
                    row["FirstName"] if row["FirstName"] else "",
                    row["SurName"] if row["SurName"] else "",
                    row["Sex"] if row["Sex"] else ""
                ))
            logger.info(f"Loaded {len(df)} records with missing data")
        except Exception as e:
            logger.error(f"Error loading missing data: {str(e)}")
            messagebox.showerror("Error", f"Failed to load data: {str(e)}")

    def on_select(self, event):
        """Handle selection of a row"""
        selected = self.tree.selection()
        if selected:
            item = self.tree.item(selected[0])
            values = item["values"]
            self.first_name_entry.delete(0, tk.END)
            self.first_name_entry.insert(0, values[2])
            self.surname_entry.delete(0, tk.END)
            self.surname_entry.insert(0, values[3])
            self.sex_entry.delete(0, tk.END)
            self.sex_entry.insert(0, values[4])
            self.selected_patient_id = values[0]
    
    def save_updates(self):
        """Save the updated data to the database and transition if all records are updated"""
        if not hasattr(self, "selected_patient_id"):
            messagebox.showwarning("Warning", "No patient selected")
            return

        first_name = self.first_name_entry.get().strip()
        surname = self.surname_entry.get().strip()
        sex = self.sex_entry.get().strip()

        if not all([first_name, surname, sex]):
            messagebox.showwarning("Warning", "All fields must be filled")
            return

        # Use SQLAlchemy named parameters with colons instead of %s
        update_query = """
        UPDATE tblpatients
        SET FirstName = :first_name, SurName = :surname, Sex = :sex
        WHERE PatientID = :patient_id
        """
        try:
            # Pass parameters as a dictionary with named parameters
            params = {
                'first_name': first_name,
                'surname': surname,
                'sex': sex,
                'patient_id': self.selected_patient_id
            }
            self.source_conn.execute_update(update_query, params)
            logger.info(f"Updated patient {self.selected_patient_id}")
            # Update treeview
            selected = self.tree.selection()[0]
            self.tree.item(selected, values=(self.selected_patient_id, self.tree.item(selected)["values"][1], first_name, surname, sex))
            messagebox.showinfo("Success", "Data updated successfully")
        
            # Remove the updated item from the treeview
            self.tree.delete(selected)
        
            # Check if there are any remaining items
            if not self.tree.get_children():
                logger.info("All patient records updated, transitioning to MDE upload")
                self.window.destroy()  # Close the validator GUI
                # Launch MDE uploader directly
                reference_manager = [None]
                def set_reference_manager(ref_mgr):
                    reference_manager[0] = ref_mgr
                mde_uploader = MDEUploaderGUI(set_reference_manager)
                mde_uploader.run()
                # Store the reference manager in the instance for main.py to access
                self.reference_manager = reference_manager[0]

        except Exception as e:
            logger.error(f"Error updating data: {str(e)}")
            messagebox.showerror("Error", f"Failed to update data: {str(e)}")
        
    def run(self):
        """Run the GUI"""
        self.window.mainloop()
        # Return the reference manager if set
        return getattr(self, 'reference_manager', None)

    def has_missing_data(self):
        """Check if there is any missing data"""
        query = """
        SELECT COUNT(*) as count
        FROM tblpatients
        WHERE FirstName IS NULL OR FirstName = ''
        OR SurName IS NULL OR SurName = ''
        OR Sex IS NULL OR Sex = ''
        """
        result = self.source_conn.execute_query(query)
        return result["count"].iloc[0] > 0