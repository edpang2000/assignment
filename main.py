import time
import os
import re
import csv
from datetime import datetime

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from watchdog.events import PatternMatchingEventHandler

from db_stuff import Inserter

# folder to observe for changes
path = os.path.join(os.path.dirname(os.path.realpath('__file__')),'incoming')

# regex for grabbing the filename from the watchdog Handler full path
re_filename_ptn = re.compile(r"[^\\]+$")

class Handler(PatternMatchingEventHandler):
    # Watches directories for file changes

    def __init__(self):
        PatternMatchingEventHandler.__init__(self,patterns=['*.SMRT'],ignore_patterns = None,ignore_directories = True, case_sensitive = False)

    def on_modified(self,event):
        # When files are modified
        pass

    def on_created(self,event):
        # When files are created

        filename = ''.join([i.group(0) for i in re_filename_ptn.finditer(event.src_path)])

        # permission error sometimes happen if you try to read when the file was created at the same time, hence...
        time.sleep(0.00001)

        # ignore files that are not .SMRT
        if filename.upper().endswith(".SMRT"):
            # if we're still getting PermissionError because that sleep above isn't long enough we'll just dirty infinite loop it
            while True:
                try:
                    with open(os.path.join(path,filename),'r') as csv_file:
                        csv_reader = csv.reader(csv_file)
                        lst_csv_reader = list(csv_reader)
                        break
                except PermissionError as e:
                    time.sleep(0.00001)
                    continue

            # file validity check
            if lst_csv_reader[0][0] != "HEADR" or lst_csv_reader[-1][0] != "TRAIL":
                print(f"{filename} has invalid header or footer!")
                return
            elif lst_csv_reader[0][0] == "HEADR" and lst_csv_reader[-1][0] == "TRAIL":
                print(f"{filename} is valid")

                # grab this value from header, used as foreign key on the consumptions table
                file_generation_number_val = lst_csv_reader[0][5]

                # insert into headers and consumptions in one transaction
                Inserter(
                    lst_headers = lst_csv_reader[0][1:3] + [datetime.now().strftime("%Y%m%d")] + [datetime.now().strftime("%H%M%S")] + [file_generation_number_val]
                    ,lst_consumptions = [[file_generation_number_val] + i[1:] for i in lst_csv_reader[1:-1]]
                ).insert()

        elif not filename.upper().endswith(".SMRT"):
            print('This is not an SMRT file!')

    def on_deleted(self,event):
        # When files are deleted
        pass

    def on_moved(self,event):
        # When files are moved
        pass

if __name__ == "__main__":
    # Initialise file system event handler
    fs_event_handler = FileSystemEventHandler()

    # Calling funcs
    fs_event_handler.on_modified = Handler().on_modified
    fs_event_handler.on_created = Handler().on_created
    fs_event_handler.on_deleted = Handler().on_deleted
    fs_event_handler.on_moved = Handler().on_moved

    # Initialise Observer
    observer_fs = Observer()
    observer_fs.schedule(fs_event_handler, path, recursive=False)

    # Start the observer observer_fs
    observer_fs.start()
    try:
        print("")
        print("######## SMRT File Watcher: Active ########")
        print(f"Put your SMRT files here -> {path}...")
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer_fs.stop()
        print("Done")
    observer_fs.join()