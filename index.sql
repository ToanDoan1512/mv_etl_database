CREATE UNIQUE INDEX idx_bc_co_hoi_row_id ON public.bc_co_hoi USING btree (row_id);

CREATE UNIQUE INDEX idx_bc_so_lien_he_row_id ON public.bc_so_lien_he USING btree (row_id);

CREATE UNIQUE INDEX country_type_pkey ON public.country_type USING btree (id);

CREATE UNIQUE INDEX crm_group_pkey ON public.crm_group USING btree (id);

CREATE UNIQUE INDEX crm_lead_pkey ON public.crm_lead USING btree (id);

CREATE INDEX idx_crm_lead_create_date ON public.crm_lead USING btree (create_date);

CREATE INDEX idx_crm_lead_crm_group_id ON public.crm_lead USING btree (crm_group_id);

CREATE UNIQUE INDEX idx_crm_lead_id ON public.crm_lead USING btree (id);

CREATE INDEX idx_crm_lead_partner_id ON public.crm_lead USING btree (partner_id);

CREATE INDEX idx_crm_lead_sale_crm_lead_id ON public.crm_lead USING btree (sale_crm_lead_id);

CREATE UNIQUE INDEX crm_lead_note2_pkey ON public.crm_lead_note2 USING btree (id);

CREATE INDEX idx_crm_lead_note2_crm_lead_id ON public.crm_lead_note2 USING btree (crm_lead_id);

CREATE UNIQUE INDEX crm_team_pkey ON public.crm_team USING btree (id);

CREATE UNIQUE INDEX crmf99_system_pkey ON public.crmf99_system USING btree (id);

CREATE UNIQUE INDEX idx_data_raw_bc_so_lien_he_row_id ON public.data_raw_bc_so_lien_he USING btree (row_id);

CREATE UNIQUE INDEX idx_data_raw_etl_chi_tiet_don_hang_row_id ON public.data_raw_etl_chi_tiet_don_hang USING btree (row_id);

CREATE INDEX idx_data_raw_etl_create_date ON public.data_raw_etl_chi_tiet_don_hang USING btree (create_date);

CREATE UNIQUE INDEX idx_data_raw_etl_don_hang_row_id ON public.data_raw_etl_don_hang USING btree (row_id);

CREATE INDEX idx_etl_chi_tiet_don_hang_create_date ON public.etl_chi_tiet_don_hang USING btree (create_date);

CREATE INDEX idx_etl_chi_tiet_don_hang_product_id ON public.etl_chi_tiet_don_hang USING btree (product_id);

CREATE UNIQUE INDEX idx_etl_chi_tiet_don_hang_row_id ON public.etl_chi_tiet_don_hang USING btree (row_id);

CREATE UNIQUE INDEX idx_etl_don_hang_row_id ON public.etl_don_hang USING btree (row_id);

CREATE UNIQUE INDEX hr_daily_expense_pkey ON public.hr_daily_expense USING btree (id);

CREATE UNIQUE INDEX hr_employee_pkey ON public.hr_employee USING btree (id);

CREATE UNIQUE INDEX kol_pkey ON public.kol USING btree (id);

CREATE UNIQUE INDEX product_category_pkey ON public.product_category USING btree (id);

CREATE UNIQUE INDEX product_product_pkey ON public.product_product USING btree (id);

CREATE INDEX idx_res_partner_active ON public.res_partner USING btree (active);

CREATE INDEX idx_res_partner_country_type_id ON public.res_partner USING btree (country_type_id);

CREATE INDEX idx_res_partner_create_date ON public.res_partner USING btree (create_date);

CREATE INDEX idx_res_partner_id ON public.res_partner USING btree (id);

CREATE INDEX idx_res_partner_source_id ON public.res_partner USING btree (source_id);

CREATE UNIQUE INDEX res_partner_pkey ON public.res_partner USING btree (id);

CREATE UNIQUE INDEX res_users_pkey ON public.res_users USING btree (id);

CREATE INDEX idx_sale_order_create_date ON public.sale_order USING btree (create_date);

CREATE UNIQUE INDEX idx_sale_order_id ON public.sale_order USING btree (id);

CREATE INDEX idx_sale_order_opportunity_id ON public.sale_order USING btree (opportunity_id);

CREATE INDEX idx_sale_order_partner_id ON public.sale_order USING btree (partner_id);

CREATE UNIQUE INDEX sale_order_pkey ON public.sale_order USING btree (id);

CREATE INDEX idx_sale_order_line_order_id ON public.sale_order_line USING btree (order_id);

CREATE INDEX idx_sale_order_line_product_id ON public.sale_order_line USING btree (product_id);

CREATE UNIQUE INDEX sale_order_line_pkey ON public.sale_order_line USING btree (id);

CREATE UNIQUE INDEX idx_tab_bc_co_hoi_row_id ON public.tab_bc_co_hoi USING btree (row_id);

CREATE UNIQUE INDEX idx_tab_note_bc_co_hoi_row_id ON public.tab_note_bc_co_hoi USING btree (row_id);

CREATE UNIQUE INDEX utm_channel_pkey ON public.utm_channel USING btree (id);

CREATE UNIQUE INDEX idx_utm_source_id ON public.utm_source USING btree (id);

CREATE UNIQUE INDEX utm_source_pkey ON public.utm_source USING btree (id);