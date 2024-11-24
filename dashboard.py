import os
import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
from datetime import datetime

class SalesDashboard:
    def __init__(self, output_dir="./output"):
        self.output_dir = output_dir  # Dossier contenant les fichiers de sortie Spark
        self.app = dash.Dash(__name__)
        self.setup_layout()
        self.setup_callbacks()
        
    def load_data(self, start_date, end_date):
        """
        Charge les données des fichiers entre start_date et end_date
        """
        try:
            all_data = []
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            
            for filename in os.listdir(self.output_dir):
                if filename.endswith('.txt'):
                    # Extraire la date du fichier
                    file_date = datetime.strptime(filename[:8], '%Y%m%d')
                    
                    if start.date() <= file_date.date() <= end.date():
                        filepath = os.path.join(self.output_dir, filename)
                        try:
                            # Lire le fichier en respectant le format des logs
                            df_temp = pd.read_csv(filepath, sep='|', 
                                                  names=['datetime', 'id', 'product', 'sales'],
                                                  encoding='utf-8')
                            df_temp['sales'] = pd.to_numeric(df_temp['sales'], errors='coerce')
                            all_data.append(df_temp)
                        except Exception as e:
                            print(f"Erreur lors de la lecture du fichier {filename}: {e}")
                            continue
            
            if all_data:
                df = pd.concat(all_data, ignore_index=True)
                df['date'] = pd.to_datetime(df['datetime']).dt.date
                df['hour'] = pd.to_datetime(df['datetime']).dt.hour
                return df
            return pd.DataFrame()
        except Exception as e:
            print(f"Erreur lors du chargement des données: {e}")
            return pd.DataFrame()

    def setup_layout(self):
        """
        Configure la mise en page du dashboard
        """
        self.app.layout = html.Div([
            html.H1('Dashboard Analyse des Logs - Ventes', style={'textAlign': 'center'}),
            
            # Sélecteurs de date
            html.Div([
                html.Label('Sélectionner une période:'),
                dcc.DatePickerRange(
                    id='date-picker',
                    min_date_allowed=datetime(2024, 1, 1),
                    max_date_allowed=datetime(2024, 12, 31),
                    start_date=datetime(2024, 11, 23),
                    end_date=datetime(2024, 11, 24)
                )
            ], style={'marginBottom': 20}),
            
            # Statistiques
            html.Div([
                html.Div([
                    html.H4('Ventes Totales'),
                    html.H3(id='total-sales')
                ], className='stat-card'),
                html.Div([
                    html.H4('Nombre de Produits Uniques'),
                    html.H3(id='product-count')
                ], className='stat-card'),
                html.Div([
                    html.H4('Produit le Plus Vendu'),
                    html.H3(id='top-product')
                ], className='stat-card')
            ], style={'display': 'flex', 'justifyContent': 'space-around'}),
            
            # Graphiques
            dcc.Graph(id='sales-evolution-chart'),
            dcc.Graph(id='top-products-chart'),
            dcc.Graph(id='hourly-sales-chart'),
            dcc.Graph(id='product-share-chart')
        ])

    def setup_callbacks(self):
        """
        Configure les callbacks pour la mise à jour dynamique
        """
        @self.app.callback(
            [Output('total-sales', 'children'),
             Output('product-count', 'children'),
             Output('top-product', 'children'),
             Output('sales-evolution-chart', 'figure'),
             Output('top-products-chart', 'figure'),
             Output('hourly-sales-chart', 'figure'),
             Output('product-share-chart', 'figure')],
            [Input('date-picker', 'start_date'),
             Input('date-picker', 'end_date')]
        )
        def update_dashboard(start_date, end_date):
            if not start_date or not end_date:
                return "0 €", "0", "Aucun", {}, {}, {}, {}

            # Charger les données
            df = self.load_data(start_date, end_date)
            if df.empty:
                return "0 €", "0", "Aucun", {}, {}, {}, {}

            # Calculer les statistiques
            total_sales = f"{df['sales'].sum():,.2f} €"
            product_count = str(df['product'].nunique())
            top_product = df.groupby('product')['sales'].sum().idxmax()

            # 1. Graphique Top Produits
            top_products = df.groupby('product')['sales'].sum().nlargest(5)
            fig1 = px.bar(
                x=top_products.values,
                y=top_products.index,
                orientation='h',
                title="Top 5 Produits par Ventes"
            )

            # 2. Évolution des ventes
            sales_evolution = df.groupby('date')['sales'].sum().reset_index()
            fig2 = px.line(
                sales_evolution,
                x='date',
                y='sales',
                title="Évolution des Ventes"
            )

            # 3. Ventes par heure
            hourly_sales = df.groupby('hour')['sales'].sum().reset_index()
            fig3 = px.bar(
                hourly_sales,
                x='hour',
                y='sales',
                title="Ventes par Heure"
            )

            # 4. Répartition des ventes
            product_share = df.groupby('product')['sales'].sum().reset_index()
            fig4 = px.pie(
                product_share,
                names='product',
                values='sales',
                title="Répartition des Ventes par Produit"
            )

            return total_sales, product_count, top_product, fig1, fig2, fig3, fig4

    def run_server(self, debug=True, port=8050):
        """
        Lance le serveur Dash
        """
        self.app.run_server(debug=debug, port=port)


if __name__ == '__main__':
    dashboard = SalesDashboard(output_dir="./output")  # Chemin des fichiers Spark
    dashboard.run_server()
