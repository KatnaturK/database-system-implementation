
#include "Main.h"

void Main::run() {
  char *fileName = "Statistics.txt";
  Statistics s;
  Ddl d; 
  QueryPlan plan(&s);
  int choice ;
  bool connected = false;

  while(true) {
    cout << "\n1: Fire up the Database" << endl;
    cout << "2: Query Database" << endl;
    cout << "3: Close Database" << endl;
    cout << "Enter Option:          $";
    cin >> choice;

    switch(choice) {
      case 1: 
          if(connected) {
            cout << "\n--- Database Already Connected ---\n" << endl;
            continue;
          }
          cout << "\n--- Database Connection Established ---\n" << endl;
	        connected = true;
          break;

      case 2:
          if(!connected) {
            cout << "\n--- Connection Not Established Yet---\n" << endl;
            continue;
          }
          cout << "\nEnter SET OUTPUT <STDOUT(default)>, <'file_name'>, <NONE>" << endl;
          cout << "                       OR                                  " << endl;
          cout << "Enter Query CNF(enter ';' at the end).                   \n" << endl;
          if (yyparse() != 0) {
            cout << "\nCan't parse your CNF.\n";
            continue;
          }

        s.Read(fileName);

        if (newtable) {
          if (d.createTable()) cout << "Create table " << newtable << endl;
          else cout << "Table " << newtable << " already exists." << endl;
        } else if (oldtable && newfile) {
          if (d.insertInto()) cout << "Insert into " << oldtable << endl;
          else cout << "Insert failed." << endl;
        } else if (oldtable && !newfile) {
          if (d.dropTable()) cout << "Drop table " << oldtable << endl;
          else cout << "Table " << oldtable << " does not exist." << endl;
        } else if (deoutput) {
          plan.setOutput(deoutput);
        } else if (attsToSelect || finalFunction) {
          plan.plan();
          plan.print();
          plan.execute();
        }
        clear();
        break;

      case 3:
          if(!connected)
            cout<<"\n--- Connection Not Established Yet---\n"<<endl;
	        else {
            cout<< "\n--- Database Shutdown ---\n"<<endl;
	          exit(0);
          }
          break;

      default: 
        cout << "\n--- Enter a valid option ----" << endl;
    }
  }
}

// TODO: free lists
void Main::clear() {
  newattrs = NULL;
  finalFunction = NULL;
  tables = NULL;
  boolean = NULL;
  groupingAtts = NULL;
  attsToSelect = NULL;
  newtable = oldtable = newfile = deoutput = NULL;
  distinctAtts = distinctFunc = 0;
  FATALIF (!remove ("*.tmp"), "remove tmp files failed");
}
