# Best Value for Money Products on Amazon

## Overview

This project analyzes **value-for-money (VFM)** across Amazon products using **PySpark** and a large-scale dataset of product metadata and customer reviews. It aims to uncover how well product ratings align with actual affordability and customer satisfaction.

## Objectives

- Identify top VFM products in categories like **Electronics** and **CD & Vinyl**
- Quantify the percentage of **high-rated products** that are genuinely good value
- Use **NLP techniques** to analyze **customer review text** and extract key factors influencing perceived value (e.g., price, durability, quality)
- Compare value-for-money perceptions across different product categories

## Research Questions

1. **RQ1**: How do products in the “Electronics” category compare in terms of value-for-money?  
2. **RQ2**: What percentage of high-rated products are actually good value for money?  
3. **RQ3**: What aspects mentioned in the high-rated reviews of the "Electronics" category most influence customers' perceptions of value-for-money?  
4. **RQ4**: How do products in the "CD & Vinyl" category compare to those in the "Electronics" category in terms of value-for-money, and what similarities or striking differences can be observed?

## Tech Stack

- **Apache Spark / PySpark**
- **Python**
- **NLP**
- **Amazon Product & Review Dataset**

## Key Files

- `RQ1_and_RQ4.py`: Calculates VFM scores and compares categories
- `RQ2.py`: Determines the percentage of highly-rated products that are actually good value
- `RQ3_electronics.py` & `RQ3_cdvinyl.py`: NLP analysis to extract aspects influencing VFM from customer reviews

## Formula Used

```python
Value for Money (VFM) = (Average Rating × log(Number of Reviews + 1)) / Price
```

## Results Summary
- Only ~40% of highly-rated electronics products offer good value
- Top aspects influencing value perception: price, quality, performance, features
- Electronics generally show higher and more consistent VFM compared to CD & Vinyl
