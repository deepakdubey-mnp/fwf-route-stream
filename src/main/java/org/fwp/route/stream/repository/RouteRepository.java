package org.fwp.route.stream.repository;

import org.fwp.route.stream.entity.RouteEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RouteRepository extends JpaRepository<RouteEntity, String> {
}
