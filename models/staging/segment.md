{% docs order_status %}

One of the following values explains:
| status     | definition                                                                                  |
|------------|---------------------------------------------------------------------------------------------|
| Cancelled  | Order that has been officially voided or revoked before it is fulfilled or shipped          |
| Complete   | Order has been received by the customers                                                    |
| Processing | The order details are reviewed, payment is verified, and the order is prepared for shipment |
| Returned   | Items has been Returned                                                                     |
| Shipped    | Order has been shipped but has not yet been delivered                                       |

{% enddocs %}



{% docs event_type %}

One of the following values explains:
| event           | definition                                                                                                                                 |
|-----------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| cancel          | The action taken by a customer to cancel an order or a placed transaction                                                                  |
| cart            | Cart events are important for tracking users behavior and understanding the shopping patterns of customers                                 |
| department      | Department events are used to track users interest in different product categories and analyze the performance of various departments      |
| home            | The action taken by a user when they visit the home page of an online store, it provide insights into user traffic patterns                |
| product         | This analyze product performance, identify popular products, and personalize product recommendations to improve the shopping experience    |
| purchase        | This data is valuable for analyzing sales performance, understanding customer buying behavior, and optimizing marketing strategies         |

{% enddocs %}



{% docs traffic_source %}

One of the following values explains:
| traffic_source  | definition                                                                                                                                          |
|-----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| Display         | The traffic that comes from display advertising such as banner ads, media, and other graphical ads                                                  |
| Email           | The traffic from email marketing campaigns such as promotional emails, newsletters, and other marketing emails sent to customers or subscribers     |
| Facebook        | Traffic from Facebook such as organic posts, paid advertisements, and other content on Facebook that directs users to the ecommerce website         |
| Organic         | This traffic source are from visitors who reach the site by clicking on a search engine result, rather than clicking on a paid advertisement        |      
| Search          | visitors who land as a result of a search engine query. This includes both organic search results (unpaid) and paid search results (advertisements) |

{% enddocs %}