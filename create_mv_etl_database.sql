-- Tạo view ETL chi tiết đơn hàng
CREATE MATERIALIZED VIEW data_raw_etl_chi_tiet_don_hang AS
select
	row_number() OVER () AS row_id
    , date_trunc('day', sale_order.create_date + interval '7 hour') create_date
    , date_trunc('day', sale_order.confirmed_datetime + interval '7 hour') confirmed_datetime
    , date_part('hour', sale_order.confirmed_datetime + interval '7 hour') confirmed_hour
    , date_trunc('day', sale_order.latest_done_pick_datetime + interval '7 hour') latest_done_pick_datetime
    , date_trunc('day', sale_order.latest_done_out_datetime + interval '7 hour') latest_done_out_datetime
    , date_trunc('day', sale_order.latest_done_pick_return_datetime + interval '7 hour') latest_done_pick_return_datetime
    , date_trunc('day', sale_order.ngaydonghang + interval '7 hour') ngaydonghang
    , res_partner.country_type_id
    , sale_order.shipping_address_type 
    , sale_order.product_category_id 
    , sale_order.source_id
    , sale_order.user_id
    , sale_order.team_id 
    , sale_order.crm_group_id 
    , sale_order.summary_state
    , sale_order.warehouse_id 
    , case 
		when res_partner.customer_type= 'wholesale' then 'Sỉ'
		when  res_partner.customer_type= 'tmtd' then 'Thương mại điện tử'
		else 'Lẻ'
	end phan_loai_khach
	, sale_order.opportunity_type
	, sale_order.currency_id
	, sale_order_line.thanh_tien_noi_dia 
    , sale_order.id order_id 
    , crm_lead_sale.user_id sale_user_id 
    , crm_lead_sale.crm_group_id sale_crm_group_id 
    , date_trunc('day', crm_lead.create_date + interval '7 hour') create_date_lead
    , sale_order_line.product_id 
    , sale_order_line.product_uom_qty 
    , sale_order_line.qty_delivered
    , sale_order_line.fixed_amount_discount
    , sale_order_line.price_subtotal
from 
    sale_order_line
    left join sale_order on sale_order.id = sale_order_line.order_id
    left join res_partner on sale_order.partner_id = res_partner.id 
    left join crm_lead on crm_lead.id = sale_order.opportunity_id
    left join crm_lead crm_lead_sale on crm_lead_sale.id = crm_lead.sale_crm_lead_id
where
    sale_order.create_date > date_trunc('year', current_date) + interval '- 7 hour - 1 second';

-- Tạo unique index trên row_id
CREATE UNIQUE INDEX idx_data_raw_etl_chi_tiet_don_hang_row_id ON data_raw_etl_chi_tiet_don_hang (row_id);

CREATE MATERIALIZED VIEW etl_chi_tiet_don_hang AS
select 
    row_number() OVER () AS row_id
    , create_date
    , confirmed_datetime
    , confirmed_hour
    , latest_done_pick_datetime
    , latest_done_out_datetime
    , latest_done_pick_return_datetime
    , ngaydonghang
    , country_type_id
    , shipping_address_type 
    , product_category_id 
    , source_id
    , user_id
    , team_id 
    , crm_group_id 
    , summary_state
    , warehouse_id 
    , phan_loai_khach
    , opportunity_type
    , currency_id
    , sale_user_id
    , sale_crm_group_id
    , create_date_lead
    , product_id
    , sum(thanh_tien_noi_dia) thanh_tien_noi_dia
    , sum(price_subtotal) price_subtotal
    , count(distinct order_id) n_order 
    , sum(product_uom_qty) product_uom_qty
    , sum(qty_delivered) qty_delivered
    , sum(fixed_amount_discount) fixed_amount_discount
from data_raw_etl_chi_tiet_don_hang 
group by 
	create_date
    , confirmed_datetime
    , confirmed_hour
    , latest_done_pick_datetime
    , latest_done_out_datetime
    , latest_done_pick_return_datetime
    , ngaydonghang
    , country_type_id
    , shipping_address_type 
    , product_category_id 
    , source_id
    , user_id
    , team_id 
    , crm_group_id 
    , summary_state
    , warehouse_id 
    , phan_loai_khach
    , currency_id
    , opportunity_type
    , sale_user_id
    , sale_crm_group_id
    , create_date_lead
    , product_id
    
CREATE UNIQUE INDEX idx_etl_chi_tiet_don_hang_row_id ON etl_chi_tiet_don_hang (row_id);

-- Tạo view ETL dơn hàng
CREATE MATERIALIZED VIEW data_raw_etl_don_hang AS
select
    row_number() OVER () AS row_id
    , date_trunc('day', sale_order.create_date + interval '7 hour') create_date
    , date_trunc('day', sale_order.confirmed_datetime + interval '7 hour') confirmed_datetime
    , date_part('hour', sale_order.confirmed_datetime + interval '7 hour') confirmed_hour
    , date_trunc('day', sale_order.latest_done_pick_datetime + interval '7 hour') latest_done_pick_datetime
    , date_trunc('day', sale_order.latest_done_out_datetime + interval '7 hour') latest_done_out_datetime
    , date_trunc('day', sale_order.latest_done_pick_return_datetime + interval '7 hour') latest_done_pick_return_datetime
    , date_trunc('day', sale_order.ngaydonghang + interval '7 hour') ngaydonghang
    , res_partner.country_type_id
    , sale_order.shipping_address_type 
    , sale_order.product_category_id 
    , sale_order.source_id
    , sale_order.user_id
    , sale_order.team_id 
    , sale_order.crm_group_id 
    , sale_order.summary_state
    , sale_order.warehouse_id 
    , case 
		when res_partner.customer_type= 'wholesale' then 'Sỉ'
		when  res_partner.customer_type= 'tmtd' then 'Thương mại điện tử'
		else 'Lẻ'
	end phan_loai_khach
	, sale_order.opportunity_type
	, sale_order.currency_id
	, sale_order.amount_total price_subtotal
    , case when sale_order.currency_id= 23 then sale_order.amount_total else sale_order.amount_total/sale_order.currency_rate end thanh_tien_noi_dia
    , sale_order.id order_id 
    , crm_lead_sale.user_id sale_user_id 
    , crm_lead_sale.crm_group_id sale_crm_group_id 
    , date_trunc('day', crm_lead.create_date + interval '7 hour') create_date_lead
    , (select state from sale_order_operating where sale_order_operating.sale_order_id = sale_order.id order by sale_order_operating.create_date desc limit 1) state
from  sale_order  
    left join res_partner on sale_order.partner_id = res_partner.id 
    left join crm_lead on crm_lead.id = sale_order.opportunity_id
    left join crm_lead crm_lead_sale on crm_lead_sale.id = crm_lead.sale_crm_lead_id
where
    sale_order.create_date > date_trunc('month', current_date) + interval '-12 month';

-- Tạo unique index trên row_id
CREATE UNIQUE INDEX idx_data_raw_etl_don_hang_row_id ON data_raw_etl_don_hang (row_id);

CREATE MATERIALIZED VIEW etl_don_hang AS
select 
    row_number() OVER () AS row_id
    , create_date
    , confirmed_datetime
    , confirmed_hour
    , latest_done_pick_datetime
    , latest_done_out_datetime
    , latest_done_pick_return_datetime
    , ngaydonghang
    , country_type_id
    , shipping_address_type 
    , product_category_id 
    , source_id
    , user_id
    , team_id 
    , crm_group_id 
    , summary_state
    , warehouse_id 
    , phan_loai_khach
    , opportunity_type
    , currency_id
    , sale_user_id
    , sale_crm_group_id
    , create_date_lead
    , state
    , sum(thanh_tien_noi_dia) thanh_tien_noi_dia
    , sum(price_subtotal) price_subtotal
    , count(distinct order_id) n_order 
from data_raw_etl_don_hang 
group by 
	create_date
    , confirmed_datetime
    , confirmed_hour
    , latest_done_pick_datetime
    , latest_done_out_datetime
    , latest_done_pick_return_datetime
    , ngaydonghang
    , country_type_id
    , shipping_address_type 
    , product_category_id 
    , source_id
    , user_id
    , team_id 
    , crm_group_id 
    , summary_state
    , warehouse_id 
    , phan_loai_khach
    , currency_id
    , opportunity_type
    , sale_user_id
    , sale_crm_group_id
    , create_date_lead
    , state

-- Tạo unique index trên row_id
CREATE UNIQUE INDEX idx_etl_don_hang_row_id ON etl_don_hang (row_id);
    
-- Tạo view bc so lien he
CREATE MATERIALIZED VIEW data_raw_bc_so_lien_he AS
select
    row_number() OVER () AS row_id,
    res_partner.id,  
    date_trunc('day', res_partner.create_date + interval '+7 hour') create_date,
    date_part('hour', res_partner.create_date + interval '+7 hour') create_hour,
    res_partner.was_closed, 
    res_partner.contact_creator_id, 
    res_partner.marketing_team_id team_id, 
    res_partner.crm_group_id, 
    res_partner.crmf99_system_id, 
    utm_source.channel_id, 
    res_partner.country_type_id, 
    res_partner.product_category_id, 
    utm_source.user_id source_user_id, 
    utm_source.crm_group_id source_crm_group_id, 
    utm_source.id source_id
from res_partner
    left join utm_source on res_partner.source_id=utm_source.id
where
    res_partner.active is true

-- Tạo unique index trên row_id
CREATE UNIQUE INDEX idx_data_raw_bc_so_lien_he_row_id ON data_raw_bc_so_lien_he (row_id);

CREATE MATERIALIZED VIEW bc_so_lien_he AS
select 
    row_number() OVER () AS row_id
    , create_date
    , create_hour
    , contact_creator_id
    , team_id
    , product_category_id
    , crm_group_id
    , crmf99_system_id
    , channel_id
    , country_type_id
    , source_user_id
    , source_crm_group_id
    , source_id
    , count(distinct id) tong_sdt 
    , count(distinct id) filter(where was_closed is null or was_closed is false) sdt 
from data_raw_bc_so_lien_he
group by 
    create_date
    , create_hour
    , contact_creator_id
    , team_id
    , product_category_id
    , crm_group_id
    , crmf99_system_id
    , channel_id
    , country_type_id
    , source_user_id
    , source_crm_group_id
    , source_id

-- Tạo unique index trên row_id
CREATE UNIQUE INDEX idx_bc_so_lien_he_row_id ON bc_so_lien_he (row_id);

-- Tạo view bc co hoi
CREATE MATERIALIZED VIEW tab_note_bc_co_hoi AS
select 
    row_number() OVER () AS row_id,
    crm_lead_id, 
    count(crm_lead_note2.id) lead_note 
from crm_lead_note2
    left join crm_lead on crm_lead.id = crm_lead_note2.crm_lead_id
group by crm_lead_id
-- Tạo unique index trên crm_lead_id
CREATE UNIQUE INDEX idx_tab_note_bc_co_hoi_row_id ON tab_note_bc_co_hoi (row_id);

CREATE MATERIALIZED VIEW tab_bc_co_hoi AS
select 
    row_number() OVER () AS row_id
    , rm_lead.id lead_id 
    , crm_lead.first_user_id
    , crm_lead.user_id
    , crm_group.id crm_group_id 
    , product_category.id product_category_id
    , country_type.id country_type_id 
    , date_trunc('day', crm_lead.first_date_open + interval '7 hour') first_date_open
    , date_trunc('day', crm_lead.date_open + interval '7 hour') date_open
    , date_trunc('day', crm_lead.create_date + interval '7 hour') create_date
    , crm_lead_note2.lead_note 
    , utm_source.channel_id
    , case 
    	when res_partner.customer_type= 'wholesale' then 'Sỉ'
    	when  res_partner.customer_type= 'tmtd' then 'Thương mại điện tử'
    	else 'Lẻ'
    end phan_loai_khach
    , utm_source.id source_id
from 
    crm_lead 
    left join crm_group on crm_group.id = crm_lead.crm_group_id 
    left join crmf99_system on crmf99_system.id = crm_group.crmf99_system_id 
    left join product_category on product_category.id = crm_lead.product_category_id
    left join res_partner on res_partner.id = crm_lead.partner_id
    left join utm_source on utm_source.id = res_partner.source_id
    left join country_type on country_type.id = res_partner.country_type_id 
    left join tab_note_bc_co_hoi crm_lead_note2 on crm_lead_note2.crm_lead_id = crm_lead.id 
where 
    crm_lead.active= 'true'
    and crm_lead.opportunity_type= 'sale';

-- Tạo unique index trên row_id
CREATE UNIQUE INDEX idx_tab_bc_co_hoi_row_id ON tab_bc_co_hoi (row_id);


CREATE MATERIALIZED VIEW bc_co_hoi AS
select 
    row_number() OVER () AS row_id
    , count(lead_id) so_co_hoi 
    , count(lead_id) filter(where lead_note is not null) da_goi
    , first_date_open
    , date_open
    , create_date
    , first_user_id
    , user_id
    , crm_group_id
    , country_type_id
    , product_category_id 
    , channel_id
    , phan_loai_khach
    , source_id
 from tab_bc_co_hoi 
 group by first_date_open
    , date_open
    , create_date
    , first_user_id
    , user_id
    , crm_group_id
    , product_category_id
    , channel_id
    , phan_loai_khach
    , country_type_id
    , source_id;

-- Tạo unique index trên row_id
CREATE UNIQUE INDEX idx_bc_co_hoi_row_id ON bc_co_hoi (row_id);