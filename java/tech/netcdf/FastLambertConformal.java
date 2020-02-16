package tech.netcdf;

import ucar.unidata.geoloc.LatLonPoint;
import ucar.unidata.geoloc.LatLonPointImpl;
import ucar.unidata.geoloc.ProjectionPointImpl;
import ucar.unidata.geoloc.ProjectionPoint;
import ucar.unidata.geoloc.ProjectionImpl;
import ucar.unidata.geoloc.projection.LambertConformal;

public class FastLambertConformal
{
  public final double lon0Degrees;
  public final double PI_OVER_4;
  public final double earthRadiusTimesF;
  public final double falseEasting;
  public final double falseNorthing;
  public final double rho;
  public final double n;
  public final double F;
  public static final double TOLERANCE = 1.0e-6;
  public static final double EARTH_RADIUS = ProjectionImpl.EARTH_RADIUS;
  public FastLambertConformal( LambertConformal proj )
  {
    lon0Degrees = proj.getOriginLon();
    PI_OVER_4 = (Math.PI / 4);
    falseEasting = proj.getFalseEasting();
    falseNorthing = proj.getFalseNorthing();
    double par1 = proj.getParallelOne();
    double par2 = proj.getParallelTwo();

    double par1r = Math.toRadians(par1);
    double par2r = Math.toRadians(par2);

    double t1 = Math.tan(Math.PI / 4 + par1r / 2);
    double t2 = Math.tan(Math.PI / 4 + par2r / 2);

    if (Math.abs(par2 - par1) < TOLERANCE) {  // single parallel
      n = Math.sin(par1r);
    } else {
      n = Math.log(Math.cos(par1r) / Math.cos(par2r))
              / Math.log(t2 / t1);
    }

    double t1n = Math.pow(t1, n);
    F = Math.cos(par1r) * t1n / n;
    earthRadiusTimesF = ProjectionImpl.EARTH_RADIUS * F;
    double lat0 = Math.toRadians(proj.getOriginLat());
    double t0n = Math.pow(Math.tan(Math.PI / 4 + lat0 / 2), n);
    rho = earthRadiusTimesF / t0n;
  }

  public static double lonNormal(double val)
  {
    while(val > 180.0)
      val -= 360.0;
    while(val < -180.0)
      val += 360.0;
    return val;
  }

  public ProjectionPoint
    latLonToProj(LatLonPoint latLon,
		 ProjectionPointImpl result)
  {
    double toX, toY;
    double fromLat = latLon.getLatitude();
    double fromLon = latLon.getLongitude();

    fromLat = Math.toRadians(fromLat);
    double dlon = lonNormal(fromLon - lon0Degrees);
    double theta = n * Math.toRadians(dlon);
    double tn = Math.pow(Math.tan(PI_OVER_4 + fromLat / 2), n);
    double r = earthRadiusTimesF / tn;
    toX = r * Math.sin(theta);
    toY = rho - r * Math.cos(theta);

    result.setLocation(toX + falseEasting, toY + falseNorthing);
    return result;
  }
}
