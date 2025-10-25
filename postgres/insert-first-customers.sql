INSERT INTO customers (name, birthday, membership_level, shipping_address) 
VALUES
('John Doe', '1990-01-01', 'free', '123 Main St, Anytown, USA'),
('Jane Smith', '1985-06-15', 'pro', '5678 Maple St, Anytown, USA'),
('Jim Beam', '1972-12-09', 'elite', '7890 Oak St, Anytown, USA');

INSERT INTO orders (customer_id, product, cost, description, credit_card_number)
VALUES
(1, 'Product A', 100.00, 'Description A', '4111111111111111'),
(2, 'Product B', 200.00, 'Description B', '4222222222222222'),
(3, 'Product C', 300.00, 'Description C', '4333333333333333');
