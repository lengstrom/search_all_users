#!/bin/bash

#ami-0ee1c464
aws ec2 run-instances --image-id "ami-0ee1c464" --count "1" --instance-type "t2.micro" --key-name "aws_key" --security-groups "sg-b1af26da"
