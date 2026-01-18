import SwaggerUI from 'swagger-ui-react';
import 'swagger-ui-react/swagger-ui.css';

export default function ApiDocs() {
  return (
    <div style={{
      height: '100%',
      overflow: 'auto',
      background: 'var(--bg-primary)',
    }}>
      <style>{`
        /* Custom Swagger UI theme to match DuckLake dark theme */
        .swagger-ui {
          font-family: var(--font-sans);
        }
        .swagger-ui .topbar {
          display: none;
        }
        .swagger-ui .info {
          margin: 20px 0;
        }
        .swagger-ui .info .title {
          font-size: 28px;
          font-weight: 700;
        }
        .swagger-ui .info .description p {
          font-size: 14px;
          line-height: 1.6;
        }
        .swagger-ui .opblock-tag {
          font-size: 18px;
          font-weight: 600;
          border-bottom: 1px solid var(--border-light);
        }
        .swagger-ui .opblock {
          border-radius: 8px;
          box-shadow: none;
          border: 1px solid var(--border-light);
          margin-bottom: 12px;
        }
        .swagger-ui .opblock .opblock-summary {
          padding: 8px 16px;
        }
        .swagger-ui .opblock .opblock-summary-method {
          border-radius: 4px;
          font-size: 12px;
          font-weight: 600;
          min-width: 70px;
        }
        .swagger-ui .opblock.opblock-get .opblock-summary-method {
          background: var(--accent-secondary);
        }
        .swagger-ui .opblock.opblock-post .opblock-summary-method {
          background: var(--success);
        }
        .swagger-ui .opblock.opblock-put .opblock-summary-method,
        .swagger-ui .opblock.opblock-patch .opblock-summary-method {
          background: var(--warning);
        }
        .swagger-ui .opblock.opblock-delete .opblock-summary-method {
          background: var(--error);
        }
        .swagger-ui .opblock .opblock-summary-path {
          font-family: var(--font-mono);
          font-size: 13px;
        }
        .swagger-ui .opblock .opblock-summary-description {
          font-size: 13px;
          color: var(--text-secondary);
        }
        .swagger-ui .opblock-body pre {
          border-radius: 6px;
          font-family: var(--font-mono);
          font-size: 12px;
        }
        .swagger-ui .btn {
          border-radius: 6px;
          font-weight: 500;
        }
        .swagger-ui .btn.execute {
          background: var(--accent-primary);
          border-color: var(--accent-primary);
        }
        .swagger-ui .btn.execute:hover {
          background: var(--accent-primary-hover);
        }
        .swagger-ui select {
          border-radius: 6px;
        }
        .swagger-ui input[type=text],
        .swagger-ui textarea {
          border-radius: 6px;
          font-family: var(--font-mono);
        }
        .swagger-ui .model-box {
          border-radius: 6px;
        }
        .swagger-ui table tbody tr td {
          padding: 10px;
        }
        .swagger-ui .parameter__name {
          font-family: var(--font-mono);
          font-size: 13px;
        }
        .swagger-ui .parameter__type {
          font-family: var(--font-mono);
          font-size: 12px;
          color: var(--accent-secondary);
        }
        .swagger-ui .response-col_status {
          font-family: var(--font-mono);
          font-size: 13px;
        }
        .swagger-ui .servers > label {
          font-size: 13px;
          font-weight: 500;
        }
        .swagger-ui .servers select {
          font-size: 13px;
        }
        .swagger-ui .scheme-container {
          background: var(--bg-secondary);
          padding: 16px;
          border-radius: 8px;
        }
        .swagger-ui section.models {
          border: 1px solid var(--border-light);
          border-radius: 8px;
        }
        .swagger-ui section.models h4 {
          font-size: 16px;
          font-weight: 600;
        }
        .swagger-ui .model-title {
          font-family: var(--font-mono);
          font-size: 14px;
        }
        .swagger-ui .markdown code,
        .swagger-ui .renderedMarkdown code {
          background: var(--bg-tertiary);
          padding: 2px 6px;
          border-radius: 4px;
          font-family: var(--font-mono);
          font-size: 12px;
        }
        .swagger-ui .markdown pre,
        .swagger-ui .renderedMarkdown pre {
          background: var(--bg-tertiary);
          border-radius: 6px;
          padding: 12px;
        }
      `}</style>
      <SwaggerUI
        url="/openapi.yaml"
        docExpansion="list"
        defaultModelsExpandDepth={1}
        displayRequestDuration={true}
        filter={true}
        showExtensions={true}
        showCommonExtensions={true}
        tryItOutEnabled={false}
      />
    </div>
  );
}
