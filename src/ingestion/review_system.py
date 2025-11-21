import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker

fake =Faker()
def generate_customer_reviews(num_reviews=3000):
    """
    Generate customer reviews in Parquet format with:
    - Text data with special characters
    - Missing values
    - Spam/fake reviews
    - Inconsistent ratings
    """
    
    reviews = []
    start_date = datetime(2023, 1, 1)
    
    # Review templates for realism
    positive_templates = [
        "Great product! Exceeded my expectations. Would definitely recommend.",
        "Excellent quality and fast shipping. Very satisfied with this purchase.",
        "Amazing! This is exactly what I was looking for. Five stars!",
        "Love it! Works perfectly and the price was reasonable.",
        "Best purchase I've made in a while. Highly recommended."
    ]
    
    negative_templates = [
        "Disappointed with the quality. Not worth the money.",
        "Product arrived damaged and customer service was unhelpful.",
        "Don't waste your money. Cheap quality and doesn't work as advertised.",
        "Terrible experience. Would not recommend to anyone.",
        "Overpriced and underdelivered. Very disappointed."
    ]
    
    neutral_templates = [
        "It's okay. Nothing special but does the job.",
        "Average product. Met basic expectations but nothing more.",
        "Decent for the price. Has some pros and cons.",
        "Not bad, not great. Just average overall."
    ]
    
    spam_templates = [
        "Click here for amazing deals!!! www.spam.com",
        "BUY NOW!!! BEST PRICE GUARANTEED!!!",
        "FREE GIFT!!! LIMITED TIME OFFER!!!",
        "🔥🔥🔥 HOT DEAL 🔥🔥🔥"
    ]
    
    for i in range(num_reviews):
        review_id = f"REV{str(i+10000).zfill(8)}"
        
        # Some duplicate reviews (data quality issue)
        if random.random() < 0.01 and i > 0:
            review_id = reviews[-1]['review_id']
        
        product_id = f"PROD{random.randint(1, 500):06d}"
        customer_id = f"CUST{random.randint(1, 2000):06d}"
        
        # Missing customer IDs (anonymous reviews)
        if random.random() < 0.05:
            customer_id = None
        
        # Review date
        review_date = start_date + timedelta(days=random.randint(0, 680))
        
        # Rating with inconsistencies
        if random.random() > 0.02:
            rating = random.randint(1, 5)
        else:
            # Invalid ratings
            rating = random.choice([0, 6, -1, None, ''])
        
        # Select review text based on rating
        if rating and rating >= 4:
            review_text = random.choice(positive_templates)
        elif rating and rating <= 2:
            review_text = random.choice(negative_templates)
        elif rating == 3:
            review_text = random.choice(neutral_templates)
        else:
            review_text = "No comment"
        
        # Introduce spam reviews
        if random.random() < 0.02:
            review_text = random.choice(spam_templates)
            rating = 5  # Spam usually gives high ratings
        
        # Add variations and special characters
        if random.random() > 0.7:
            review_text += " " + random.choice([
                "👍", "😊", "❤️", "⭐⭐⭐⭐⭐",
                "!!!!", "???", "...",
                "<script>alert('xss')</script>",  # XSS attempt
                "'; DROP TABLE reviews;--"  # SQL injection attempt
            ])
        
        # Review title
        title = f"Review for {product_id}"
        if random.random() > 0.5:
            title = random.choice([
                "Great!", "Disappointed", "Just okay", "Amazing product",
                "Not recommended", "Best purchase ever", "Waste of money",
                "", None
            ])
        
        # Verified purchase flag with inconsistencies
        verified_purchase = random.choice([
            True, False, 'true', 'false', 'True', 'False',
            1, 0, 'Y', 'N', 'Yes', 'No', None, ''
        ])
        
        # Helpful votes
        helpful_votes = random.randint(1, 100) if random.random() > 0.1 else None
        total_votes = helpful_votes + random.randint(0, 50) if helpful_votes else None
        
        # Reviewer name with PII issues
        if random.random() > 0.3:
            reviewer_name = f"User{random.randint(1, 5000)}"
        else:
            # Real names (PII that should be anonymized)
            reviewer_name = random.choice([
                "John Smith", "Jane Doe", "Anonymous", "amazon_customer_123",
                "", None
            ])
        
        reviews.append({
            'review_id': review_id,
            'product_id': product_id,
            'customer_id': customer_id,
            'rating': rating,
            'review_title': title,
            'review_text': review_text,
            'review_date': review_date,
            'verified_purchase': verified_purchase,
            'helpful_votes': helpful_votes,
            'total_votes': total_votes,
            'reviewer_name': reviewer_name,
            'review_length': len(review_text) if review_text else 0,
            'contains_images': random.choice([True, False, None]),
            'contains_video': random.choice([True, False, None]),
            'review_status': random.choice([
                'published', 'Published', 'pending', 'rejected',
                'flagged', '', None
            ])
        })
    
    return pd.DataFrame(reviews)


# Generate reviews
print("Generating customer reviews data...")
reviews_df = generate_customer_reviews(3000)


# Ensure rating is numeric
reviews_df['rating'] = pd.to_numeric(reviews_df['rating'], errors='coerce').astype('Int64')

# Ensure helpful_votes and total_votes are nullable integers
reviews_df['helpful_votes'] = reviews_df['helpful_votes'].astype('Int64')
reviews_df['total_votes'] = reviews_df['total_votes'].astype('Int64')

# Convert verified_purchase to boolean type
def normalize_boolean(x):
    if str(x).lower() in ['true', '1', 'y', 'yes']:
        return True
    elif str(x).lower() in ['false', '0', 'n', 'no']:
        return False
    else:
        return pd.NA
reviews_df['verified_purchase'] = reviews_df['verified_purchase'].apply(normalize_boolean).astype('boolean')

# Save as Parquet (efficient columnar format)
reviews_df.to_parquet('customer_reviews.parquet', engine='pyarrow', compression='snappy')
print(f"✓ Generated customer_reviews.parquet with {len(reviews_df)} reviews")

# Also save as CSV for comparison
reviews_df.to_csv('customer_reviews.csv', index=False)
print("✓ Generated customer_reviews.csv (same data in CSV format)")

print("\n" + "="*60)
print("REVIEWS DATA CHARACTERISTICS:")
print("="*60)
print("✓ Text data with special characters and emojis")
print("✓ Potential spam and fake reviews")
print("✓ PII data that needs anonymization")
print("✓ Security threats (XSS, SQL injection attempts)")
print("✓ Invalid ratings (out of range)")
print("✓ Missing values in various fields")
print("✓ Inconsistent boolean representations")
print("✓ Duplicate reviews")

print("\n" + "="*60)
print("RATING DISTRIBUTION:")
print("="*60)
print(reviews_df['rating'].value_counts().sort_index())

print("\n" + "="*60)
print("VERIFIED PURCHASE VARIATIONS:")
print("="*60)
print(reviews_df['verified_purchase'].value_counts())

print("\n" + "="*60)
print("SAMPLE REVIEWS:")
print("="*60)
print(reviews_df[['review_id', 'product_id', 'rating', 'review_title', 'review_text']].head(5))

print("\n" + "="*60)
print("PARQUET FILE INFO:")
print("="*60)
print(f"Number of rows: {len(reviews_df)}")
print(f"Number of columns: {len(reviews_df.columns)}")
print(f"Memory usage: {reviews_df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
print("\nColumn dtypes:")
print(reviews_df.dtypes)