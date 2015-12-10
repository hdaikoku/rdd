//
// Created by Harunobu Daikoku on 2015/12/09.
//

#ifndef PROJECT_BI_GRAM_REDUCER_H
#define PROJECT_BI_GRAM_REDUCER_H

#include <string>
#include <reducer.h>

class BiGramReducer
    : public Reducer<std::pair<std::string, std::string>, int, std::pair<std::string, std::string>, int> {

 public:

  virtual std::pair<std::pair<std::string, std::string>, int> Reduce(const std::pair<std::string, std::string> &key,
                                                                     const std::vector<int> &values) override;
};

extern "C" std::unique_ptr<Reducer<std::pair<std::string, std::string>,
                                   int,
                                   std::pair<std::string, std::string>,
                                   int>> Create() {
  return std::unique_ptr<Reducer<std::pair<std::string, std::string>, int, std::pair<std::string, std::string>, int>>(
      new BiGramReducer);
}

#endif //PROJECT_BI_GRAM_REDUCER_H
