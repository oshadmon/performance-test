# Performance Testing

**Steps**
0. Install requirements 
```shell
python3 -m pip install --upgrade pip 
python3 -m pip install -r ./requirements.txt
```
1. Enable  [aggregation](set_aggregations.py) to gather insights
```python
python3 set_aggregations.py [comma separated Operator connection] --num-columns 10 --dbms [DB Name] --table [Table Name]  
```
