/**
 * @file main.cpp
 * @author your name (you@domain.com)
 * @brief 
 * @version 0.1
 * @date 2025-11-18
 * 
 * @copyright Copyright (c) 2025
 * 
 */
#include <Arduino.h>

/**
 * @brief testing funtction
 * 
 */
// put function declarations here:
int myFunction(int, int);

/**
 * @brief testing setup
 * 
 */
void setup() {
  // put your setup code here, to run once:
  int result = myFunction(2, 3);
}

/**
 * @brief testing loop
 * 
 */
void loop() {
  // put your main code here, to run repeatedly:
}

/**
 * @brief testing function definition
 * 
 */
// put function definitions here:
int myFunction(int x, int y) {
  return x + y;
}