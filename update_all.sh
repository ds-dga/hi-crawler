#!/usr/bin/env sh


export PED_APIKEY=d74881c2-20cb-49ae-872b-afa6bce2f139
export BU_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjYxMWJhNjljYjIxYzJhNzE3MGY4N2IyOCIsInJvbGUiOjQwLCJpYXQiOjE2MjkyMDIwNzZ9.oGWvO4h1f4Zs7sxuXUr3OgcGNO_ccHOn_IsKvz6_-FU

cd $HOME/dev/dga/hi

/Users/sipp11/.virtualenvs/hi/bin/python ped.py
/Users/sipp11/.virtualenvs/hi/bin/python wesafe.py
/Users/sipp11/.virtualenvs/hi/bin/python nectec.py

