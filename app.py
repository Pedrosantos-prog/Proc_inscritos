from server import connect_bd, close_bd
import pandas as pd
import openpyxl
import streamlit as st
import io

def proc_inscricoes(id_evento):
    """Process event registrations and return DataFrame"""
    connection = connect_bd()
    cursor = connection.cursor()

    query = """
                        SELECT
            b.additional_data as "N. Peito",
            b.product_type,
            b.sku,
            b.product_id as B_ID,
            g.product_id as G_ID, 
            p.value as "ID Evento",
            j.`value` as "Evento",
            null as "Local Inscrição",
            null as "Balcão",
            a.entity_id,
            a.increment_id as "Protocolo",
            o.all_orders_ids,
            b.item_id as "ID Inscrição",
            e.id as "id customer related",
            date(k.value) as "Data Evento",
            date(a.created_at) as "Data Pedido",
            a.state as "Status Pedido",
            a.`status` as "Status Confirmado",
            s.VALUE as "Modalidade",
            j.attribute_id,
            b.`name`as "Categoria",
            if( c.group_id = 4 , "SIM" , "NÃO") as "Assinante",
            b.ext_order_item_id as "Pelotão",
            c.entity_id as "ID Usuario",
            json_value(b.ozone_customers,'$[0].id'),
            concat(c.firstname," ",c.lastname) as "Nome inscrição" ,
            IF (
                SUBSTRING_INDEX(c.firstname, " ", 1) = " ",
                SUBSTRING_INDEX(c.firstname, " ", 2),
                SUBSTRING_INDEX(c.firstname, " ", 1)
            ) as "Primeiro Nome",
            c.email as "e-mail",
            i.telephone as "Telefone",
            i.telephone as "Celular",
            c.dob as "Dt Nascimento",
            c.taxvat as "Documento",
            Case 
            WHEN c.gender = 1 THEN "M"
            WHEN c.gender = 2 THEN "F"
            ELSE c.gender END as "Sexo",
            null as "Tamanho Camiseta",
            null as "Ordem Revezamento",

            if(json_value(b.ozone_customers, '$[0].custom_1') = "null" , null , json_value(b.ozone_customers, '$[0].custom_1')) as "Nome Camiseta",


            aa.name as "Produto Personalização",
            a.applied_rule_ids as "ID Cupom",
            a.coupon_code as "Cupom",
            (select nn.label from salesrule_label as nn where f.rule_id = nn.rule_id and nn.store_id = 0 ) as "Etiqueta 0",
            (select nn.label from salesrule_label as nn where f.rule_id = nn.rule_id and nn.store_id = 1 ) as "Etiqueta 1",
            f.name as "Nome Cupom",
            b.price as "Inscrição Bruta",
            if(a.base_discount_invoiced is null , 0 , a.base_discount_invoiced) * (b.price/ a.base_subtotal)  as "Desconto",
            l.total_amount / (SELECT COUNT(*) from sales_order_item as ba where ba.order_id = b.order_id and ba.product_type = 'Bundle' group by ba.order_id ) as "Taxa Bruta",
            if( b.price = 0 ,0 , b.price + if(a.base_discount_invoiced is null , 0 , a.base_discount_invoiced)  * (b.price/ a.base_subtotal)) as "Inscrição Liquida",
            l.total_amount / (SELECT COUNT(*) from sales_order_item as ba where ba.order_id = b.order_id and ba.product_type = 'Bundle' group by ba.order_id ) as "Taxa Liquida",
            null as "Valor Optin",
            null as "Nome Optin",
            (
            SELECT
                d.attribute_code
            FROM
            catalog_product_entity_int as a
            RIGHT JOIN catalog_product_entity as b ON a.entity_id = b.entity_id
            LEFT JOIN eav_attribute_option_value as c ON a.VALUE = c.option_id
            LEFT JOIN eav_attribute as d ON a.attribute_id = d.attribute_id
            WHERE
            a.attribute_id = 194 and a.entity_id = b.product_id
            ) as "combo",
            null as "1° evento combo",
            q.`value` as "ds_titulo",
            REPLACE(concat(m.firstname," ",m.lastname), "  " , " ") as "Nome Comprador",
            m.taxvat as "Documento Comprador",
            i.city AS "ds_cidade",
            i.street "ds_endereco",
            i.postcode AS "ds_cep",
            i.region as "Estado",
            b.ozone_customers,
            a.location_pickup_id
            FROM
            sales_order as a
            LEFT JOIN sales_order_item as b ON b.order_id = a.entity_id
            LEFT JOIN customer_has_related_order_items as e on e.sales_order_item_item_id = b.item_id
            LEFT JOIN customer_entity as c on e.customer_id = c.entity_id
            LEFT JOIN customer_entity as m on a.customer_id = m.entity_id
            LEFT JOIN catalog_product_link as g on b.product_id = g.linked_product_id
            LEFT JOIN	sales_order_address as h on a.entity_id = h.parent_id
            LEFT JOIN customer_address_entity as i on m.entity_id = i.parent_id
            LEFT JOIN amasty_extrafee_order as l on a.entity_id = l.order_id
            LEFT JOIN salesrule as f on a.applied_rule_ids = f.rule_id
            LEFT JOIN wk_customwork_suborder_list as o on a.entity_id = o.parent_order_id
            LEFT JOIN catalog_product_entity_varchar as p on 
            if(
            (
            SELECT
                d.attribute_code
            FROM
            catalog_product_entity_int as a
            RIGHT JOIN catalog_product_entity as b ON a.entity_id = b.entity_id
            LEFT JOIN eav_attribute_option_value as c ON a.VALUE = c.option_id
            LEFT JOIN eav_attribute as d ON a.attribute_id = d.attribute_id
            WHERE
            a.attribute_id = 194 and a.entity_id = b.product_id
            ) = "event_combo" , 
            b.product_id = p.entity_id,
            g.product_id = p.entity_id)
            LEFT JOIN catalog_product_entity_varchar as j on p.value = j.entity_id
            LEFT JOIN catalog_product_entity_datetime as k on p.value = k.entity_id
            LEFT JOIN catalog_product_entity_varchar as q on g.product_id = q.entity_id
            LEFT JOIN (SELECT * from sales_order_item where name like "%persona%" ) as aa on aa.parent_item_id = b.item_id 
            -- MODALIDADE/TAMANHO
            LEFT join sales_order_item as ab on ab.parent_item_id = b.item_id
            left join catalog_product_entity_int as r on r.entity_id = ab.product_id  
            inner JOIN eav_attribute_option_value as s ON r.VALUE = s.option_id
            WHERE
            p.value in (
            %s
            ) and 
            b.product_type = 'Bundle' AND
            j.attribute_id = 73 and
            q.attribute_id = 73 and
            r.attribute_id = 206 and
            (k.attribute_id = 195 or k.attribute_id is null) and
            p.attribute_id = 321 and 
            h.address_type = "shipping" and 
            a.status in ('Processing' ,'Complete' , 'approved','aprovado_link', 'reembolso_parcial')
            GROUP BY b.item_id
            ORDER BY		
            a.increment_id DESC
            """
    
    try:
        parametro = (id_evento,)
        cursor.execute(query, parametro)
        resultados = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df_inscritos = pd.DataFrame(resultados, columns=columns)
        
        # Optional: Save to local file as backup
        df_inscritos.to_excel('inscritos.xlsx', index=False)
        
        return df_inscritos
        
    except Exception as e:
        raise Exception(f"Erro na consulta ao banco de dados: {str(e)}")
    finally:
        close_bd(connection)

def proc_eventos():
    """Get list of available events"""
    connection = connect_bd()
    cursor = connection.cursor()

    query = """
            SELECT
	a.sku,
	j.entity_id AS 'Id_evento',
	j.`value` AS 'Evento',
	date(k.`value`) AS 'Data',
	b.value AS 'Status',
	c.value AS 'Tipo'
    FROM
	`catalog_product_entity` AS a
    left join catalog_product_entity_int as b on a.entity_id = b.entity_id
    LEFT JOIN catalog_product_entity_varchar AS j ON a.entity_id = j.entity_id
    LEFT JOIN catalog_product_entity_datetime AS k ON j.entity_id = k.entity_id
    LEFT JOIN	`catalog_product_entity_int` as d on a.entity_id = d.entity_id
    LEFT JOIN eav_attribute_option_value as c ON d.VALUE = c.option_id
    WHERE
	k.`value` > '2024-01-01'
    AND a.attribute_set_id = 23
    AND j.attribute_id = 73
    AND k.attribute_id = 195
    and b.attribute_id = 320
    and d.attribute_id = 199
    and b.value = 1
    ORDER BY
	k.`value`
            """
    try:
        cursor.execute(query)
        resultados = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df_eventos = pd.DataFrame(resultados, columns=columns)
        return df_eventos
    except Exception as e:
        raise Exception(f"Erro ao buscar eventos: {str(e)}")
    finally:
        close_bd(connection)

def convert_df_to_excel(df):
    """Convert DataFrame to Excel bytes for download"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Inscritos')
    return output.getvalue()

################################

st.set_page_config(page_title="Exportar Inscritos", layout="centered")

st.title("📋 Exportador de Inscritos")

# Optional: Show available events
if st.checkbox("Mostrar eventos disponíveis"):
    try:
        with st.spinner("Carregando eventos..."):
            df_eventos = proc_eventos()
            if not df_eventos.empty:
                st.subheader("Eventos Disponíveis")
                st.dataframe(df_eventos)
            else:
                st.info("Nenhum evento encontrado.")
    except Exception as e:
        st.error(f"Erro ao carregar eventos: {str(e)}")

# Campo de entrada para o ID do evento
id_evento = st.text_input("Digite o ID do Evento", placeholder="Ex: 28940")

# Quando o botão for clicado
if st.button("Exportar para Excel"):
    if not id_evento.strip():
        st.error("Por favor, insira um ID de evento.")
    elif not id_evento.strip().isdigit():
        st.error("Por favor, insira um número válido para o ID do evento.")
    else:
        with st.spinner("Consultando e gerando planilha..."):
            try:
                df_resultado = proc_inscricoes(id_evento)
                
                if df_resultado.empty:
                    st.warning("Nenhum inscrito encontrado para esse evento.")
                else:
                    st.success(f"Planilha gerada com sucesso! Encontrados {len(df_resultado)} inscritos.")
                    
                    # Convert DataFrame to Excel bytes
                    excel_data = convert_df_to_excel(df_resultado)
                    
                    # Download button
                    st.download_button(
                        label="📥 Baixar Excel",
                        data=excel_data,
                        file_name=f"inscritos_evento_{id_evento}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    
                    # Show preview of the data
                    st.subheader("Preview dos Dados")
                    st.dataframe(df_resultado)
                    
            except Exception as e:
                st.error(f"Erro ao gerar planilha: {str(e)}")

# Add some helpful information
st.markdown("---")
st.markdown("**💡 Dicas:**")
st.markdown("- Digite apenas o número do ID do evento")
st.markdown("- O sistema buscará por inscrições com status: Processing, Complete, approved, aprovado_link, reembolso_parcial")
st.markdown("- A planilha será gerada em formato Excel (.xlsx)")