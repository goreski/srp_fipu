/* Ručna provjera unosa podataka u bazu podataka
Output selecta mora u potpunosti odgovarati csv podacima iz datoteke
*/

SELECT cy.name 'Retailer country'
, od.name 'Order method type'
, re.name 'Retailer type'
, pe.name 'Product type'
, pt.name 'Product'
, ss.year 'Year'
, ss.quarter 'Quarter'
, ss.revenue 'Revenue'
, ss.quantity 'Quantity'
, ss.gross_margin 'Gross margin'
FROM pentaho.product pt
, pentaho.product_type pe
, pentaho.sales ss
, pentaho.order_method od
, pentaho.retailer_type re
, pentaho.country cy
WHERE pt.product_type_fk = pe.id
AND ss.product_fk = pt.id
AND ss.order_method_fk = od.id
AND ss.retailer_type_fk = re.id
AND ss.country_fk = cy.id
ORDER BY ss.id ASC