#!/bin/bash

find . -name "*.ipynb" | xargs jupyter nbconvert --ClearOutputPreprocessor.enabled=True --inplace 
