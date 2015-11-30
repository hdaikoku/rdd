//
// Created by Harunobu Daikoku on 2015/11/27.
//

#ifndef PROJECT_WORD_COUNT_COMBINER_H
#define PROJECT_WORD_COUNT_COMBINER_H

#include <reducer.h>
#include <string>

class WordCountCombiner: public Reducer<std::string, int, std::string, int> {
 public:
  virtual std::pair<std::string, int> Reduce(const std::string &key, const std::vector<int> &values) override;
};

extern "C" std::unique_ptr<Reducer<std::string, int, std::string, int>> Create() {
  return std::unique_ptr<Reducer<std::string, int, std::string, int>>(new WordCountCombiner);
}


#endif //PROJECT_WORD_COUNT_COMBINER_H
