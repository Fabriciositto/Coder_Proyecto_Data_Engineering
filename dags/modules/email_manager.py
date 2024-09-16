from airflow.utils.email import send_email


email=None


def email_manager(context):
    # Obtiene el estado del DAG
    exception = context.get('exception', None)
    
    if exception:
        # Armo el asunto y html del email de fallo
        subject = f"Alerta de fallo: DAG {context['dag'].dag_id}"
        html_content = (
            f"<p>El DAG {context['dag'].dag_id} falló en la ejecución {context['execution_date']}.</p>"
            f"<p>Tarea fallida: {context['task_instance'].task_id}</p>"
            f"<p>Error: {exception}</p>"
            f"<p>Log URL: <a href='{context['task_instance'].log_url}'>{context['task_instance'].log_url}</a></p>"
            f"<p>Fecha de ejecución: {context['execution_date']}</p>"
            f"<p>Estado actual: {context['task_instance'].state}</p>"
        )
        send_email(to=email, subject=subject, html_content=html_content)
        
    elif context['task_instance'].task_id == 'cleanup_tmp_files':
        # Armo el asunto y html del email de éxito
        asunto = f"COMPLETADO: DAG {context['dag'].dag_id}"
        cuerpo = (
            f"<p>El DAG {context['dag'].dag_id} completó con éxito la ejecución {context['execution_date']}.</p>"
            f"<p>Fecha de ejecución: {context['execution_date']}</p>"
            f"<p>Estado final del DAG: {context['task_instance'].state}</p>"
        )
        send_email(to=email, subject=subject, html_content=html_content)

