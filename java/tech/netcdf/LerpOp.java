package tech.netcdf;


public interface LerpOp
{
  public float lerp(float lhsWeight, float lhsVal, float rhsWeight, float rhsVal);
}
