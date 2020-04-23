#include "Meta.h"
#include "DBFile.h"
#include <fstream>
#include <unistd.h>
#include "json.hpp"
#include <cstring>

char meta_path[4096];
char cur_dir[4096];
bool initFlag;

string getStringForType(Type type) {
    switch (type) {
        case Int:
            return "Int";
        case Double:
            return "Double";
        default:
            return "String";
    }
}

Type getTypeForString(string type) {
    if (type == "Int")
        return Int;
    if (type == "Double")
        return Double;
    else
        return String;
}

void init(){
    if(initFlag) return;
    initFlag= true;
    if (getcwd(cur_dir, sizeof(cur_dir)) != NULL) {
        clog << "current working dir:" << cur_dir << endl;
        strcpy(meta_path, cur_dir);
        strcat(meta_path, "/metafile.json");
    } else {
        cerr << "error while getting curent dir" << endl;
        exit(-1);
    }
}
char* GetTempPath() {
    char* temp = new char[4096];
    strcpy(temp, cur_dir);
    strcat(temp, "/metafile.json");
    return temp;
}

MetaInfo GetMetaInfo() {
    init();
    json::JSON tmp;
    fstream t(meta_path);
    string str((std::istreambuf_iterator<char>(t)),
               std::istreambuf_iterator<char>());
    tmp = tmp.Load(str);
//    cout << tmp << endl;

    MetaInfo metaInfo;
    metaInfo.binFilePath = tmp["path"].ToString();
    metaInfo.fileType = tmp["type"].ToString() == "sorted" ? sorted : heap;
    SortInfo* sortInfo=new SortInfo();
    sortInfo->runLength=tmp["runlen"].ToInt();
    OrderMaker* orderMaker=new OrderMaker();
    orderMaker->numAtts=tmp["num_atts"].ToInt();
    orderMaker->numAtts=tmp["num_atts"].ToInt();
    for (int i = 0; i < orderMaker->numAtts; ++i) {
        orderMaker->whichTypes[i] = getTypeForString(tmp["which_types"].at(i).ToString());
        orderMaker->whichAtts[i] = tmp["which_atts"].at(i).ToInt();
    }
    sortInfo->myOrder=orderMaker;
    metaInfo.sortInfo=sortInfo;
    t.close();
    return metaInfo;
}

void WriteMetaInfo(string path, fType type, void *startup) {
    init();
    FILE *out;
    json::JSON obj;
    obj["path"] = path;
    if ((out = fopen(meta_path, "w")) != NULL) {
        if (type == sorted) {
            obj["type"] = "sorted";
            SortInfo *sortInfo = (SortInfo *) startup;
            obj["runlen"] = sortInfo->runLength;
            obj["num_atts"] = sortInfo->myOrder->numAtts;
            obj["which_atts"] = json::Array(sortInfo->myOrder->whichAtts, sortInfo->myOrder->numAtts);
            string types[sortInfo->myOrder->numAtts];
            for (int i = 0; i < sortInfo->myOrder->numAtts; ++i) {
                types[i] = getStringForType(sortInfo->myOrder->whichTypes[i]);
            }
            obj["which_types"] = json::Array(types, sortInfo->myOrder->numAtts);
        } else {
            obj["type"] = "heap";
        }
        clog << "writing meta to file: " << obj << endl;
        fprintf(out, "%s", obj.dump().c_str());
        fclose(out);
    }
};