//
// Created by Harunobu Daikoku on 2016/08/31.
//

#ifndef PROJECT_SHUFFLE_SERVICE_H
#define PROJECT_SHUFFLE_SERVICE_H

class ShuffleService {
 public:
  ShuffleService() {}

  virtual void Start() = 0;
  virtual void Stop() = 0;

};

#endif //PROJECT_SHUFFLE_SERVICE_H
