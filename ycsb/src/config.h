#ifndef YCSB_SRC_CONFIG_H
#define YCSB_SRC_CONFIG_H
#include "cstdio"
#include "iostream"
#include <getopt.h>

utils::Properties gen_workload_props(const char wl_char, const utils::Properties& props) {
  utils::Properties wp(props);
  switch (wl_char) {
    case 'a': {
      wp.SetProperty("readproportion", "0.5"); 
      wp.SetProperty("updateproportion", "0.5");
      wp.SetProperty("scanproportion", "0"); 
      wp.SetProperty("insertproportion", "0");
      wp.SetProperty("workloadname", "Workload A");
      break;
    }
    case 'b': {
      wp.SetProperty("readproportion", "0.95"); 
      wp.SetProperty("updateproportion", "0.05");
      wp.SetProperty("scanproportion", "0"); 
      wp.SetProperty("insertproportion", "0");
      wp.SetProperty("workloadname", "Workload B");
      break;
    }
    case 'c': {
      wp.SetProperty("readproportion", "1.0"); 
      wp.SetProperty("updateproportion", "0");
      wp.SetProperty("scanproportion", "0"); 
      wp.SetProperty("insertproportion", "0");
      wp.SetProperty("workloadname", "Workload C");
      break;
    }
    case 'd': {
      wp.SetProperty("readproportion", "0.95"); 
      wp.SetProperty("updateproportion", "0");
      wp.SetProperty("scanproportion", "0"); 
      wp.SetProperty("insertproportion", "0.05");
      wp.SetProperty("requestdistribution", "latest");
      wp.SetProperty("workloadname", "Workload D");
      break;
    }
    case 'e': {
      wp.SetProperty("readproportion", "0"); 
      wp.SetProperty("updateproportion", "0");
      wp.SetProperty("scanproportion", "0.95"); 
      wp.SetProperty("insertproportion", "0.05");
      wp.SetProperty("workloadname", "Workload E");
      break;
    }
    case 'f': {
      wp.SetProperty("readproportion", "0.5"); 
      wp.SetProperty("updateproportion", "0");
      wp.SetProperty("scanproportion", "0.0"); 
      wp.SetProperty("insertproportion", "0");
      wp.SetProperty("readmodifywriteproportion", "0.5");
      wp.SetProperty("workloadname", "Workload F");
      break;
    }
    default: {
      fprintf(stderr, "Invalid workload character: %c\n", wl_char);
      exit(0);
    }
  }
  return wp;
}

void set_default_props(utils::Properties* props) {
	props->SetProperty("recordcount", "10000000"); // number of records
	props->SetProperty("operationcount", "10000000"); // number of operations 
	props->SetProperty("fieldlength", "100");
	props->SetProperty("fieldcount", "10");
	props->SetProperty("readallfields", "true");
	props->SetProperty("requestdistribution", "uniform");
  props->SetProperty("maxscanlength", "100");
  props->SetProperty("scanlengthdistribution", "uniform");

  props->SetProperty("threadcount", "1");
}

void parse_command_line_arguments(int argc, char* argv[], utils::Properties* props, std::vector<char>* wl_chars) {
  set_default_props(props);
  wl_chars->push_back('l');
  wl_chars->push_back('c');
  struct option long_options[] = {
    // name, has_arg, &flag, val
    { "num", required_argument, 0, 'n' },
    { "threads", required_argument, 0, 't' },
    { "workloads", required_argument, 0, 'w' },
    { "distribution", required_argument, 0, 'd'},
    { "fieldlength", required_argument, 0, 0 },
    { "fieldcount", required_argument, 0, 0 },
    { "recordcount", required_argument, 0, 0 },
    { "operationcount", required_argument, 0, 0 },
    { 0, 0, 0, 0 }
  };
  const static char* optstring = "n:t:w:d:";
  int c;
  int i;
  while ((c = getopt_long(argc, argv, optstring, long_options, &i)) != -1) {
    switch (c) {
      case 0: {
        if (long_options[i].flag != 0) {
          break;
        }
        props->SetProperty(long_options[i].name, optarg);
        break;
      }
      case 'n': {
        props->SetProperty("recordcount", optarg);
        props->SetProperty("operationcount", optarg);
        break;
      }
      case 't': {
        props->SetProperty("threadcount", optarg);
        break;
      }
      case 'w': {
        wl_chars->clear();
        std::string s(optarg);
        std::string token;
        size_t begin = 0;
        size_t end = s.find(',');
        while (end != std::string::npos) {
          token = s.substr(begin, end - begin);
          wl_chars->push_back(token.data()[0]);
          begin = end + 1;
          end = s.find(',', begin);
        }
        token = s.substr(begin, end);
        wl_chars->push_back(token.data()[0]);
        break;
      }
      case 'd': {
        props->SetProperty("requestdistribution", optarg);
        break;
      }
      default: {
        abort();
      }
    }
  }
}

#endif
