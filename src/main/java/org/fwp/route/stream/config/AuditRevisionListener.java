package org.fwp.route.stream.config;

import org.fwp.route.stream.entity.AuditRevisionEntity;
import org.hibernate.envers.RevisionListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class AuditRevisionListener implements RevisionListener {

    private String defaultUser = "system";

    @Value("${audit.default-user:system}")
    void setDefaultUser(String value) {
        this.defaultUser = value;
    }

    @Override
    public void newRevision(Object revisionEntity) {
        ((AuditRevisionEntity) revisionEntity).setUsername(defaultUser);
    }
}