/* Ručna provjera unosa podataka u bazu podataka
Output selecta mora u potpunosti odgovarati csv podacima iz datoteke
*/

use dw;

SELECT cy.name 'retailer_country'
, od.name 'order_method_type'
, re.name 'retailer_type'
, pl.name 'product_line'
, pe.name 'product_type'
, pt.name 'product'
, ss.year 'year'
, ss.quarter 'quarter'
, ss.revenue 'revenue'
, ss.quantity 'quantity'
, ss.gross_margin 'gross_margin'
FROM product pt
, product_type pe
, product_line pl
, sales ss
, order_method od
, retailer_type re
, country cy
WHERE pt.product_type_fk = pe.id
AND pe.product_line_fk = pl.id
AND ss.product_fk = pt.id
AND ss.order_method_fk = od.id
AND ss.retailer_type_fk = re.id
AND ss.country_fk = cy.id
ORDER BY ss.id ASC